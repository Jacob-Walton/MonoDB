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
#include <sys/stat.h>

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
}; /* <-- Add missing semicolon here */

/* CRC32 table for WAL record checksum */
static uint32_t crc32_table[256];

/* Initialize CRC32 table */
static void init_crc32_table() {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
        }
        crc32_table[i] = c;
    }
}

/* Calculate CRC32 of a buffer */
static uint32_t calculate_crc32(const void* data, size_t len) {
    uint32_t       crc = 0xFFFFFFFF;
    const uint8_t* buf = (const uint8_t*)data;

    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc ^ buf[i]) & 0xFF];
    }

    return ~crc;
}

/* Create a new WAL segment file */
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

#ifdef __linux__
    /* On Linux, use fallocate for preallocation */
    if (fallocate(segment->fd, 0, 0, ctx->segment_size) != 0) {
        /* Fall back to manual allocation */
        ftruncate_compat(segment->fd, ctx->segment_size);
    }
#else
    /* Use ftruncate */
    ftruncate_compat(segment->fd, ctx->segment_size);
#endif

    segment->state = WAL_SEGMENT_ACTIVE;
    return segment;
}

/* Close up and clean up a WAL segment */
static void close_segment(wal_segment_t* segment) {
    if (!segment)
        return;

    if (segment->fd >= 0) {
        close_compat(segment->fd);
        segment->fd = -1;
    }

    free(segment);
}

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
    strncpy(ctx->wal_dir, wal_dir, sizeof(ctx->wal_dir) - 1);
    ctx->segment_size      = segment_size > 0 ? segment_size : 16 * 1024 * 1024; /* Default: 16MB */
    ctx->next_segment_num  = 1; /* Start with segment number 1 */
    ctx->archived_segments = 0;

    /* Create WAL directory if it doesn't exist */
    struct stat st;
    if (stat(wal_dir, &st) != 0) {
        if (mkdir_compat(wal_dir) != 0) {
            free(ctx);
            return NULL;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        /* Path exists but is not a directory */
        free(ctx);
        return NULL;
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

        fd = open(filename, O_RDONLY);
        if (fd < 0) {
            return false; /* Failed to open segment file */
        }
    }

    /* Seed to record location */
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

bool wal_recover(wal_context_t* ctx, wal_location_t end_location) {
    /* This is a simple implementation of WAL recovery. TODO:
     * 1. Scan WAL segments looking for the last checkpoint record.
     * 2. Apply all WAL records from checkpoint to end_location.
     * 3. Roll back incomplete transactions if necessary.
     */

    /* For now, just do a checkpoint to ensure we're in a consistent state */
    return wal_checkpoint(ctx);
}