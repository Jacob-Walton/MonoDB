/**
 * @file wal.c
 * @brief Implementation of Write-Ahead Logging (WAL) system
 */

#include <errno.h>
#include <fcntl.h>
#include <monodb/core/storage/wal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_MSC_VER)
#define _CRT_SECURE_NO_WARNINGS
#pragma warning(disable:4996)   // disable deprecated function warnings
#endif

/* Platform-specific includes */
#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <windows.h>
#define mkdir_compat(path) _mkdir(path)
#define open_compat _open
#define close_compat _close
#define write_compat _write
#define read_compat _read
#define lseek_compat _lseek
#define fsync_compat _commit
#define ftruncate_compat _chsize_s
typedef __int64 ssize_t;
/* S_ISDIR macro for Windows */
#ifndef S_ISDIR
#define S_ISDIR(m)  (((m) & _S_IFMT) == _S_IFDIR)
#endif
#else
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#define mkdir_compat(path) mkdir(path, 0755)
#define open_compat open
#define close_compat close
#define write_compat write
#define read_compat read
#define lseek_compat lseek
#define fsync_compat fsync
#define ftruncate_compat ftruncate
#endif

// WAL segment metadata structure
/**
 * WAL segment file information
 */
typedef struct {
    int                 fd;             /* File descriptor */
    char                filename[256];  /* Filename of the segment */
    uint32_t            segment_num;    /* Segment number */
    wal_segment_state_t state;          /* State of the segment */
    uint32_t            current_offset; /* Current write position */
} wal_segment_t;

/**
 * WAL context structure
 */
struct wal_context_t {
    char           wal_dir[256];        /* WAL directory path */
    uint32_t       segment_size;        /* Size of each WAL segment in bytes */
    wal_segment_t* current_segment;     /* Current active segment */
    wal_location_t last_write_location; /* Last write location */

    /* Current record being built */
    wal_record_header_t* current_record;
    uint32_t             current_record_size;

    /* Control data */
    uint32_t next_segment_num;  /* Next segment number to create */
    uint32_t archived_segments; /* Number of archived segments */
    bool     initialized;       /* Initialization flag */
};

/* Recovery-related structures */
typedef struct {
    transaction_info_t* transactions;
    int count;
    int capacity;
} transaction_map_t;

/* CRC32 lookup table */
static uint32_t crc32_table[256];

/* Initialize CRC32 lookup table */
static void init_crc32_table() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
        }
        crc32_table[i] = c;
    }
}

/* Compute CRC32 checksum for the given buffer */
static uint32_t calculate_crc32(const void* data, size_t len) {
    uint32_t       crc = 0xFFFFFFFF;
    const uint8_t* buf = (const uint8_t*)data;

    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc ^ buf[i]) & 0xFF];
    }

    return ~crc;
}

/* Allocate and initialize a new WAL segment */
static wal_segment_t* create_new_segment(wal_context_t* ctx) {
    wal_segment_t* segment = (wal_segment_t*)malloc(sizeof(wal_segment_t));
    if (!segment)
        return NULL;

    segment->segment_num    = ctx->next_segment_num++;
    segment->current_offset = 0;
    segment->state          = WAL_SEGMENT_EMPTY;

    /* Create filename: 000000010000000000000001 */
    snprintf(segment->filename, sizeof(segment->filename), "%s/%08X%08X%08X", ctx->wal_dir,
             (segment->segment_num / 0xFFFFFFFF), (segment->segment_num / 0xFFFF) & 0xFFFF,
             segment->segment_num & 0xFFFF);

    /* Open the file */
    segment->fd = open_compat(segment->filename, O_CREAT | O_RDWR, 0644);
    if (segment->fd < 0) {
        free(segment);
        return NULL;
    }

    /* Preallocate data */
    ftruncate_compat(segment->fd, ctx->segment_size);

    segment->state = WAL_SEGMENT_ACTIVE;
    return segment;
}

/* Release resources for a WAL segment */
static void close_segment(wal_segment_t* segment) {
    if (!segment)
        return;

    if (segment->fd >= 0) {
        close_compat(segment->fd);
        segment->fd = -1;
    }

    free(segment);
}

/* Returns true if the specified path exists and is a directory */
static bool directory_exists(const char* path) {
    #ifdef _WIN32
    DWORD attrs = GetFileAttributes(path);
    return (attrs != INVALID_FILE_ATTRIBUTES && 
            (attrs & FILE_ATTRIBUTE_DIRECTORY));
    #else
    struct stat st;
    return (stat(path, &st) == 0 && S_ISDIR(st.st_mode));
    #endif
}

/* Allocate and initialize the transaction map */
static transaction_map_t* create_transaction_map() {
    transaction_map_t* map = (transaction_map_t*)malloc(sizeof(transaction_map_t));
    if (!map) return NULL;
    
    map->capacity = 16;  /* Start with space for 16 transactions */
    map->count = 0;
    map->transactions = (transaction_info_t*)malloc(sizeof(transaction_info_t) * map->capacity);
    
    if (!map->transactions) {
        free(map);
        return NULL;
    }
    
    return map;
}

/* Free all resources associated with the transaction map */
static void free_transaction_map(transaction_map_t* map) {
    if (map) {
        if (map->transactions) {
            free(map->transactions);
        }
        free(map);
    }
}

/* Find an existing transaction entry by XID */
static transaction_info_t* find_transaction(transaction_map_t* map, uint32_t xid) {
    for (int i = 0; i < map->count; i++) {
        if (map->transactions[i].xid == xid) {
            return &map->transactions[i];
        }
    }
    return NULL;
}

/* Add a new transaction entry and grow the map if needed */
static transaction_info_t* add_transaction(transaction_map_t* map, uint32_t xid, 
                                           wal_location_t location) {
    /* Check if we need to resize */
    if (map->count >= map->capacity) {
        int new_capacity = map->capacity * 2;
        transaction_info_t* new_transactions = 
            (transaction_info_t*)realloc(map->transactions, 
                                      sizeof(transaction_info_t) * new_capacity);
        if (!new_transactions) {
            return NULL;
        }
        map->transactions = new_transactions;
        map->capacity = new_capacity;
    }
    
    /* Add the new transaction */
    transaction_info_t* txn = &map->transactions[map->count++];
    txn->xid = xid;
    txn->state = XACT_IN_PROGRESS;
    txn->first_record = location;
    txn->last_record = location;
    
    return txn;
}

/* Get the current time in milliseconds since an arbitrary epoch */
static uint64_t get_current_time_ms() {
#ifdef _WIN32
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    ULARGE_INTEGER li;
    li.LowPart = ft.dwLowDateTime;
    li.HighPart = ft.dwHighDateTime;
    return li.QuadPart / 10000; /* Convert to milliseconds */
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
#endif
}

/* Report recovery progress at regular intervals or when forced */
static void log_recovery_progress(wal_recovery_context_t* context, bool force) {
    static uint64_t last_report_time = 0;
    static uint32_t last_processed_records = 0;
    
    uint64_t current_time = get_current_time_ms();
    
    /* Report every 5 seconds or when forced */
    if (force || last_report_time == 0 || 
        (current_time - last_report_time > 5000)) {
        
        /* Calculate records per second */
        double records_per_sec = 0;
        if (current_time > last_report_time) {
            records_per_sec = (double)(context->stats.processed_records - last_processed_records) / 
                             ((current_time - last_report_time) / 1000.0);
        }
        
        printf("Recovery progress: %u records processed, %u applied, %u skipped (%.1f records/sec)\n",
               context->stats.processed_records,
               context->stats.applied_records,
               context->stats.skipped_records,
               records_per_sec);
        
        last_report_time = current_time;
        last_processed_records = context->stats.processed_records;
    }
}

// Public: initialize WAL context and prepare first segment
wal_context_t* wal_init(const char* wal_dir, uint32_t segment_size) {
    /* Initialize CRC32 table if not already done */
    static bool crc32_initialized = false;
    if (!crc32_initialized) {
        init_crc32_table();
        crc32_initialized = true;
    }

    /* Allocate and initialize WAL context */
    wal_context_t* ctx = (wal_context_t*)malloc(sizeof(wal_context_t));
    if (!ctx)
        return NULL;

    memset(ctx, 0, sizeof(wal_context_t));
    strcpy(ctx->wal_dir, wal_dir);

    ctx->segment_size      = segment_size > 0 ? segment_size : 16 * 1024 * 1024; /* Default: 16MB */
    ctx->next_segment_num  = 1; /* Start with segment number 1 */
    ctx->archived_segments = 0;

    /* Create WAL directory if it doesn't exist */
    if (!directory_exists(wal_dir)) {
        if (mkdir_compat(wal_dir) != 0) {
            free(ctx);
            return NULL;
        }
    }

    /* Create first segment */
    ctx->current_segment = create_new_segment(ctx);
    if (!ctx->current_segment) {
        free(ctx);
        return NULL;
    }

    ctx->initialized = true;
    return ctx;
}

// Public: flush pending data and cleanly shut down the WAL system
void wal_shutdown(wal_context_t* ctx) {
    if (!ctx)
        return;

    /* Flush any pending data */
    wal_flush(ctx, true);

    /* Close the current segment */
    close_segment(ctx->current_segment);

    /* Free record buffer if any */
    if (ctx->current_record) {
        free(ctx->current_record);
    }

    free(ctx);
}

/* Begin constructing a new WAL record (returns pointer to payload area) */
void* wal_begin_record(wal_context_t* ctx, wal_record_type_t type, uint32_t xid,
                       uint16_t data_len) {
    if (!ctx || !ctx->initialized)
        return NULL;

    /* Calculate total record size */
    uint32_t total_size =
        sizeof(wal_record_header_t) + data_len + sizeof(uint32_t); /* Include CRC */

    /* Allocate memory for the record */
    if (ctx->current_record) {
        free(ctx->current_record);
    }
    ctx->current_record = (wal_record_header_t*)malloc(total_size);
    if (!ctx->current_record)
        return NULL;

    /* Initialize record header */
    ctx->current_record->total_len   = total_size;
    ctx->current_record->type        = type;
    ctx->current_record->xid         = xid;
    ctx->current_record->prev_record = ctx->last_write_location; /* Link to previous record */
    ctx->current_record->data_len    = data_len;

    ctx->current_record_size = total_size;

    /* Return pointer to data area */
    return (void*)(ctx->current_record + 1);
}

/* Finalize, checksum, and write the current record to disk */
bool wal_end_record(wal_context_t* ctx, wal_location_t* location) {
    if (!ctx || !ctx->initialized || !ctx->current_record)
        return false;

    /* Check if current segment has enough space */
    if (ctx->current_segment->current_offset + ctx->current_record_size > ctx->segment_size) {
        /* Not enough space, close current segment and create new one */
        ctx->current_segment->state = WAL_SEGMENT_FULL;
        close_segment(ctx->current_segment);

        ctx->current_segment = create_new_segment(ctx);
        if (!ctx->current_segment)
            return false;
    }

    /* Calculate record CRC and append it */
    uint32_t crc =
        calculate_crc32(ctx->current_record, ctx->current_record_size - sizeof(uint32_t));

    /* Append CRC to end of record */
    uint32_t* crc_ptr =
        (uint32_t*)((char*)ctx->current_record + ctx->current_record_size - sizeof(uint32_t));

    *crc_ptr = crc;

    /* Write record to WAL segment */
    ssize_t written =
        write_compat(ctx->current_segment->fd, ctx->current_record, ctx->current_record_size);

    if (written != (ssize_t)ctx->current_record_size) {
        return false;
    }

    /* Update last write location */
    ctx->last_write_location.segment = ctx->current_segment->segment_num;
    ctx->last_write_location.offset  = ctx->current_segment->current_offset;

    /* Update segment offset */
    ctx->current_segment->current_offset += ctx->current_record_size;

    /* If caller requested location, provide it */
    if (location) {
        *location = ctx->last_write_location;
    }

    /* Clean up record buffer */
    free(ctx->current_record);
    ctx->current_record      = NULL;
    ctx->current_record_size = 0;

    return true;
}

/* Flush WAL data to disk; wait_for_sync controls fsync vs. deferred */
bool wal_flush(wal_context_t* ctx, bool wait_for_sync) {
    if (!ctx || !ctx->initialized || !ctx->current_segment)
        return false;

    /* Ensure data is flushed to disk */
    if (wait_for_sync) {
        if (fsync_compat(ctx->current_segment->fd) != 0) {
            return false;
        }
    } else {
#ifdef __linux__
        if (fdatasync(ctx->current_segment->fd) != 0) {
            return false;
        }
#else
        if (fsync_compat(ctx->current_segment->fd) != 0) {
            return false;
        }
#endif
    }

    return true;
}

/* Write a checkpoint marker into the WAL */
bool wal_checkpoint(wal_context_t* ctx) {
    if (!ctx || !ctx->initialized)
        return false;

    /* Start a new checkpoint record */
    void* data_ptr = wal_begin_record(ctx, WAL_RECORD_CHECKPOINT, 0, 0);
    if (!data_ptr)
        return false;

    /* Write the checkpoint record */
    wal_location_t checkpoint_location;
    if (!wal_end_record(ctx, &checkpoint_location)) {
        return false;
    }

    /* Ensure checkpoint is durably written */
    if (!wal_flush(ctx, true)) {
        return false;
    }

    return true;
}

/* Read a WAL record from the given location into user buffers */
bool wal_read_record(wal_context_t* ctx, wal_location_t location, wal_record_header_t* header,
                     void* data, uint16_t data_buf_size) {
    if (!ctx || !ctx->initialized)
        return false;

    /* Check if requested location is in current segment */
    bool need_to_open = true;
    int  fd           = -1;

    if (ctx->current_segment && ctx->current_segment->segment_num == location.segment) {
        fd           = ctx->current_segment->fd;
        need_to_open = false;
    }

    if (need_to_open) {
        /* Need to open the segment file */
        char filename[256];
        snprintf(filename, sizeof(filename), "%s/%08X%08X%08X", ctx->wal_dir,
                 (location.segment / 0xFFFFFFFF), (location.segment / 0xFFFF) & 0xFFFF,
                 location.segment & 0xFFFF);

        fd = open_compat(filename, O_RDONLY, 0);
        if (fd < 0) {
            return false; /* Failed to open segment file */
        }
    }

    /* Seek to record location */
    if (lseek_compat(fd, location.offset, SEEK_SET) < 0) {
        if (need_to_open)
            close_compat(fd);
        return false; /* Failed to seek to record location */
    }

    /* Read the record header */
    wal_record_header_t hdr;
    if (read_compat(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        if (need_to_open)
            close_compat(fd);
        return false; /* Failed to read record header */
    }

    /* Validate record size */
    if (hdr.total_len < sizeof(hdr) + sizeof(uint32_t) || hdr.total_len > ctx->segment_size) {
        if (need_to_open)
            close_compat(fd);
        return false; /* Invalid record size */
    }

    /* If caller wants header, copy it */
    if (header) {
        *header = hdr;
    }

    /* If caller wants data and has provided a buffer */
    if (data && data_buf_size > 0) {
        uint16_t bytes_to_read = hdr.data_len < data_buf_size ? hdr.data_len : data_buf_size;

        if (read_compat(fd, data, bytes_to_read) != bytes_to_read) {
            if (need_to_open)
                close_compat(fd);
            return false; /* Failed to read record data */
        }
    } else if (hdr.data_len > 0) {
        /* Skip data section */
        if (lseek_compat(fd, hdr.data_len, SEEK_CUR) < 0) {
            if (need_to_open)
                close_compat(fd);
            return false; /* Failed to skip record data */
        }
    }

    /* Read CRC from end of record */
    uint32_t stored_crc;
    if (read_compat(fd, &stored_crc, sizeof(stored_crc)) != sizeof(stored_crc)) {
        if (need_to_open)
            close_compat(fd);
        return false; /* Failed to read CRC */
    }

    /* Close file if we opened it */
    if (need_to_open) {
        close_compat(fd);
    }

    return true; /* Successfully read record */
}

/* Find the most recent checkpoint location to start recovery */
static bool find_latest_checkpoint(wal_context_t* ctx, wal_location_t* checkpoint_location) {
    printf("Scanning for WAL segments in directory: %s\n", ctx->wal_dir);
    
    /* Check if directory exists and is accessible */
    if (!directory_exists(ctx->wal_dir)) {
        printf("WAL directory does not exist: %s\n", ctx->wal_dir);
        return false;
    }
    
    uint32_t highest_segment = 0;
    
    #ifdef _WIN32
    /* Windows version using FindFirstFile/FindNextFile */
    WIN32_FIND_DATA find_data;
    char search_path[260];
    snprintf(search_path, sizeof(search_path), "%s\\*", ctx->wal_dir);
    
    HANDLE find_handle = FindFirstFile(search_path, &find_data);
    if (find_handle != INVALID_HANDLE_VALUE) {
        do {
            /* Skip directories */
            if (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
                continue;
            }
            
            /* Try to parse WAL segment filename */
            uint32_t seg1 = 0, seg2 = 0, seg3 = 0;
            if (sscanf(find_data.cFileName, "%08X%08X%08X", &seg1, &seg2, &seg3) == 3) {
                printf("Found WAL segment: %s (number: %u)\n", find_data.cFileName, seg3);
                if (seg3 > highest_segment) {
                    highest_segment = seg3;
                }
            }
        } while (FindNextFile(find_handle, &find_data));
        
        FindClose(find_handle);
    } else {
        printf("Warning: Could not scan WAL directory: %s (error: %lu)\n", 
              ctx->wal_dir, GetLastError());
    }
    #else
    /* POSIX version using readdir */
    DIR* dir = opendir(ctx->wal_dir);
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != NULL) {
            /* Skip directories */
            if (entry->d_type == DT_DIR) {
                continue;
            }
            
            /* Try to parse WAL segment filename */
            uint32_t seg1 = 0, seg2 = 0, seg3 = 0;
            if (sscanf(entry->d_name, "%08X%08X%08X", &seg1, &seg2, &seg3) == 3) {
                printf("Found WAL segment: %s (number: %u)\n", entry->d_name, seg3);
                if (seg3 > highest_segment) {
                    highest_segment = seg3;
                }
            }
        }
        closedir(dir);
    } else {
        printf("Warning: Could not scan WAL directory: %s (errno: %d)\n", ctx->wal_dir, errno);
    }
    #endif
    
    if (highest_segment == 0) {
        printf("No WAL segments found in directory: %s\n", ctx->wal_dir);
        
        /* For testing purposes, let's fall back to first segment */
        checkpoint_location->segment = 1;
        checkpoint_location->offset = 0;
        return true;
    }
    
    printf("Highest segment number found: %u\n", highest_segment);
    
    /* Now scan for checkpoints */
    /* If no checkpoint found, start from beginning of first segment */
    checkpoint_location->segment = 1;
    checkpoint_location->offset = 0;
    
    return true; /* For now, always return true to attempt recovery */
}

/* Dispatch a recovered record to the registered handler */
static bool apply_recovery_record(wal_record_header_t* header, 
                                  void* data, 
                                  void* user_data) {
    wal_recovery_context_t* context = (wal_recovery_context_t*)user_data;
    wal_recovery_handlers_t* handlers = (wal_recovery_handlers_t*)context->db_instance;
    
    context->stats.processed_records++;
    context->stats.bytes_processed += header->total_len;
    
    /* Skip WAL control records */
    if (header->type == WAL_RECORD_NULL || 
        header->type == WAL_RECORD_CHECKPOINT ||
        header->type == WAL_RECORD_XACT_COMMIT ||
        header->type == WAL_RECORD_XACT_ABORT) {
        return true;
    }
    
    /* Check if we have a handler for this record type */
    if (header->type <= WAL_RECORD_SCHEMA && 
        handlers->handlers[header->type] != NULL) {
        
        /* Call the specific handler for this record type */
        if (handlers->handlers[header->type](context->db_instance, 
                                           header, 
                                           data, 
                                           header->data_len)) {
            context->stats.applied_records++;
            return true;
        }
        return false; /* Handler returned failure */
    } else {
        /* Unknown record type or no handler available */
        if (context->verbose) {
            printf("Warning: No handler for record type %d during recovery\n", header->type);
        }
        return true; /* Skip this record but continue recovery */
    }
}

/* Scan all WAL segments from a starting point and invoke callback per record */
static bool scan_records_for_recovery(wal_context_t* ctx, 
                                      wal_location_t start_location,
                                      transaction_map_t* txn_map,
                                      bool (*callback)(wal_record_header_t*, void*, void*),
                                      void* user_data) {
    
    wal_recovery_context_t* recovery_ctx = (wal_recovery_context_t*)user_data;
    
    printf("Starting WAL scan from segment %u, offset %u\n", 
           start_location.segment, start_location.offset);
    
    /* Ensure we have at least one segment */
    if (start_location.segment == 0) {
        start_location.segment = 1;
        start_location.offset = 0;
    }
    
    uint32_t current_segment = start_location.segment;
    uint32_t current_offset = start_location.offset;
    
    char segment_path[256];
    FILE* segment_file = NULL;
    
    /* Process segments sequentially */
    while (1) {
        /* Open current segment if not already open */
        if (!segment_file) {
            snprintf(segment_path, sizeof(segment_path), "%s/%08X%08X%08X", 
                     ctx->wal_dir, 
                     (current_segment / 0xFFFFFFFF),
                     (current_segment / 0xFFFF) & 0xFFFF, 
                     current_segment & 0xFFFF);
            
            segment_file = fopen(segment_path, "rb");
            if (!segment_file) {
                /* No more segments */
                break;
            }
            
            printf("Processing WAL segment: %s\n", segment_path);
            recovery_ctx->stats.processed_segments++;
        }
        
        /* Seek to current position */
        if (fseek(segment_file, current_offset, SEEK_SET) != 0) {
            printf("Error seeking to offset %u in segment %u\n", current_offset, current_segment);
            fclose(segment_file);
            return false;
        }
        
        /* Read and process records */
        while (1) {
            wal_record_header_t header;
            
            /* Read record header */
            size_t bytes_read = fread(&header, 1, sizeof(header), segment_file);
            if (bytes_read != sizeof(header)) {
                if (feof(segment_file)) {
                    /* End of segment */
                    break;
                } else {
                    /* Read error */
                    printf("Error reading record header at offset %u in segment %u\n", 
                           current_offset, current_segment);
                    fclose(segment_file);
                    return false;
                }
            }
            
            /* Validate header */
            if (header.total_len < sizeof(header) + sizeof(uint32_t) || 
                header.total_len > ctx->segment_size) {
                if (header.total_len == 0) {
                    /* Reached end of valid data */
                    printf("Reached end of valid data in segment %u at offset %u\n", 
                           current_segment, current_offset);
                } else {
                    printf("Invalid record size %u at offset %u in segment %u\n", 
                           header.total_len, current_offset, current_segment);
                }
                break;
            }
            
            /* Read record data if present */
            void* data = NULL;
            if (header.data_len > 0) {
                data = malloc(header.data_len);
                if (!data) {
                    printf("Out of memory allocating %u bytes for record data\n", header.data_len);
                    fclose(segment_file);
                    return false;
                }
                
                if (fread(data, 1, header.data_len, segment_file) != header.data_len) {
                    printf("Error reading record data at offset %zu in segment %u\n", 
                           current_offset + sizeof(header), current_segment);
                    free(data);
                    fclose(segment_file);
                    return false;
                }
            }
            
            /* Skip CRC */
            if (fseek(segment_file, sizeof(uint32_t), SEEK_CUR) != 0) {
                if (data) free(data);
                fclose(segment_file);
                return false;
            }
            
            /* Track this record in transaction map */
            if (header.xid > 0) {
                transaction_info_t* txn = find_transaction(txn_map, header.xid);
                if (!txn) {
                    wal_location_t loc = {current_segment, current_offset};
                    txn = add_transaction(txn_map, header.xid, loc);
                    if (!txn) {
                        if (data) free(data);
                        fclose(segment_file);
                        return false;
                    }
                }
                
                if (txn) {
                    /* Update transaction state */
                    if (header.type == WAL_RECORD_XACT_COMMIT) {
                        txn->state = XACT_COMMITTED;
                        recovery_ctx->stats.committed_transactions++;
                        printf("Transaction %u committed\n", header.xid);
                    } else if (header.type == WAL_RECORD_XACT_ABORT) {
                        txn->state = XACT_ABORTED;
                        recovery_ctx->stats.aborted_transactions++;
                        printf("Transaction %u aborted\n", header.xid);
                    } else {
                        /* Update last record location */
                        txn->last_record.segment = current_segment;
                        txn->last_record.offset = current_offset;
                    }
                }
            }
            
            /* Process this record using callback if it's part of a committed transaction */
            bool is_control_record = (header.type == WAL_RECORD_CHECKPOINT ||
                                    header.type == WAL_RECORD_XACT_COMMIT ||
                                    header.type == WAL_RECORD_XACT_ABORT);
            
            transaction_info_t* txn = header.xid > 0 ? find_transaction(txn_map, header.xid) : NULL;
            
            if (is_control_record || (txn && txn->state == XACT_COMMITTED)) {
                if (callback && !callback(&header, data, user_data)) {
                    printf("Error: Callback failed for record at segment %u, offset %u\n",
                          current_segment, current_offset);
                    if (data) free(data);
                    fclose(segment_file);
                    return false;
                }
            } else {
                recovery_ctx->stats.skipped_records++;
            }
            
            /* Clean up */
            if (data) {
                free(data);
            }
            
            /* Move to next record */
            current_offset += header.total_len;
            
            /* Update stats */
            recovery_ctx->stats.processed_records++;
            recovery_ctx->stats.bytes_processed += header.total_len;
            
            /* Log progress periodically */
            if (recovery_ctx->stats.processed_records % 1000 == 0) {
                log_recovery_progress(recovery_ctx, false);
            }
        }
        
        /* Close current segment and move to next */
        fclose(segment_file);
        segment_file = NULL;
        current_segment++;
        current_offset = 0;
    }
    
    /* Count incomplete transactions */
    for (int i = 0; i < txn_map->count; i++) {
        if (txn_map->transactions[i].state == XACT_IN_PROGRESS) {
            recovery_ctx->stats.incomplete_transactions++;
        }
    }
    
    /* Final progress update */
    log_recovery_progress(recovery_ctx, true);
    
    return true;
}

/* Core recovery orchestrator; locates checkpoint, scans, and applies records */
bool wal_perform_recovery(wal_context_t* ctx, wal_location_t end_location, 
                          bool (*apply_record_callback)(wal_record_header_t*, 
                                                        void* data, 
                                                        void* user_data),
                          void* user_data) {
    /* Unused parameter */
    (void)end_location;
    
    /* Find checkpoint location */
    wal_location_t start_location = {0, 0};
    if (!find_latest_checkpoint(ctx, &start_location)) {
        printf("Warning: No checkpoint found, starting from beginning\n");
        start_location.segment = 1;
        start_location.offset = 0;
    }
    
    /* Create transaction map */
    transaction_map_t* txn_map = create_transaction_map();
    if (!txn_map) {
        fprintf(stderr, "Error: Failed to create transaction map\n");
        return false;
    }
    
    /* Perform recovery scan */
    bool success = scan_records_for_recovery(ctx, start_location, txn_map, 
                                          apply_record_callback, user_data);
    
    /* Clean up */
    free_transaction_map(txn_map);
    
    return success;
}

/* Function prototypes for record handlers */
static bool handle_insert_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len);
static bool handle_update_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len);
static bool handle_delete_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len);
static bool handle_schema_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len);
static bool handle_newpage_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len);

/* Handler function implementations */
static bool handle_insert_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len) {
    (void)db; (void)hdr; (void)len;    // suppress unreferenced‐parameter warnings
    printf("Applying INSERT: \"%s\"\n", (char*)data);
    return true;
}

static bool handle_update_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len) {
    (void)db; (void)hdr; (void)len;
    printf("Applying UPDATE: \"%s\"\n", (char*)data);
    return true;
}

static bool handle_delete_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len) {
    (void)db; (void)hdr; (void)len;
    printf("Applying DELETE: \"%s\"\n", (char*)data);
    return true;
}

static bool handle_schema_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len) {
    (void)db; (void)hdr; (void)len;
    printf("Applying SCHEMA change: \"%s\"\n", (char*)data);
    return true;
}

static bool handle_newpage_record(void* db, wal_record_header_t* hdr, void* data, uint16_t len) {
    (void)db; (void)hdr; (void)data; (void)len;
    printf("Allocating new page\n");
    return true;
}

/*
 * Full database recovery function
 */
bool wal_recover(wal_context_t* ctx, wal_location_t end_location) {
    (void)end_location;   // no longer used
    if (!ctx || !ctx->initialized) {
        fprintf(stderr, "WAL system not initialized for recovery\n");
        return false;
    }
    
    printf("Starting WAL recovery...\n");
    
    /* Create recovery context */
    wal_recovery_context_t recovery_context;
    memset(&recovery_context, 0, sizeof(recovery_context));
    recovery_context.verbose = true;
    
    /* Set up handlers */
    wal_recovery_handlers_t handlers;
    memset(&handlers, 0, sizeof(handlers));
    
    /* Set up handler functions */
    handlers.handlers[WAL_RECORD_INSERT] = handle_insert_record;
    handlers.handlers[WAL_RECORD_UPDATE] = handle_update_record;
    handlers.handlers[WAL_RECORD_DELETE] = handle_delete_record;
    handlers.handlers[WAL_RECORD_SCHEMA] = handle_schema_record;
    handlers.handlers[WAL_RECORD_NEWPAGE] = handle_newpage_record;
    
    /* Set up recovery context */
    recovery_context.db_instance = &handlers;
    
    /* Start timing the recovery */
    uint64_t start_time = get_current_time_ms();
    
    /* Perform the actual recovery */
    printf("Scanning WAL for recovery...\n");
    bool success = wal_perform_recovery(ctx, end_location, apply_recovery_record, &recovery_context);
    
    /* Record recovery time */
    recovery_context.stats.recovery_time_ms = get_current_time_ms() - start_time;
    
    /* Log final recovery statistics */
    printf("Recovery %s: %u records processed, %u applied, %u skipped\n",
           success ? "completed" : "failed",
           recovery_context.stats.processed_records,
           recovery_context.stats.applied_records,
           recovery_context.stats.skipped_records);

    printf("Recovery statistics:\n");
    printf("  Segments processed: %u\n", recovery_context.stats.processed_segments);
    printf("  Records processed:  %u\n", recovery_context.stats.processed_records);
    printf("  Records applied:    %u\n", recovery_context.stats.applied_records);
    printf("  Records skipped:    %u\n", recovery_context.stats.skipped_records);
    printf("  Committed txns:     %u\n", recovery_context.stats.committed_transactions);
    printf("  Aborted txns:       %u\n", recovery_context.stats.aborted_transactions);
    printf("  Incomplete txns:    %u\n", recovery_context.stats.incomplete_transactions);

    /* use unsigned long long for 64‑bit count */
    printf("  Bytes processed:    %llu\n",
           (unsigned long long)recovery_context.stats.bytes_processed);

    printf("  Processing rate:    %.2f MB/s\n",
           (double)recovery_context.stats.bytes_processed /
           (1024.0 * 1024.0 * recovery_context.stats.recovery_time_ms / 1000.0));
    
    if (success) {
        /* Create a new checkpoint after successful recovery */
        printf("Creating post-recovery checkpoint...\n");
        if (!wal_checkpoint(ctx)) {
            fprintf(stderr, "Warning: Failed to create post-recovery checkpoint\n");
        }
    }
    
    return success;
}
