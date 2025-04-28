/**
 * @file wal.h
 * @brief Write-Ahead Logging (WAL) system for MonoDB.
 *
 * This module implements a journaling system to ensure
 * data integrity and durability in the event of a crash.
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>

/**
 * WAL record types
 */
typedef enum {
    WAL_RECORD_NULL = 0,       /* Invalid/placeholder record */
    WAL_RECORD_CHECKPOINT = 1, /* Checkpoint record */
    WAL_RECORD_XACT_COMMIT = 2,/* Transaction commit */
    WAL_RECORD_XACT_ABORT = 3, /* Transaction abort */
    WAL_RECORD_INSERT = 4,     /* Row insertion */
    WAL_RECORD_UPDATE = 5,     /* Row update */
    WAL_RECORD_DELETE = 6,     /* Row deletion */
    WAL_RECORD_NEWPAGE = 7,    /* New page allocation */
    WAL_RECORD_SCHEMA = 8      /* Schema change */
} wal_record_type_t;

/**
 * WAL segment states
 */
typedef enum {
    WAL_SEGMENT_EMPTY = 0,
    WAL_SEGMENT_ACTIVE = 1,
    WAL_SEGMENT_FULL = 2,
    WAL_SEGMENT_ARCHIVED = 3
} wal_segment_state_t;

/**
 * WAL location - comparable, sortable location identifier
 */
typedef struct {
    uint32_t segment; /* WAL segment number */
    uint32_t offset;  /* Byte offset within segment */
} wal_location_t;

/**
 * WAL record header
 */
typedef struct {
    uint32_t         total_len;   /* Total length of record including header */
    wal_record_type_t type;       /* Record type */
    uint32_t         xid;         /* Transaction ID */
    wal_location_t   prev_record; /* Previous record for this transaction */
    uint16_t         data_len;    /* Length of payload data */
    /* data follows directly after the header */
} wal_record_header_t;

/**
 * Transaction state during recovery
 */
typedef enum {
    XACT_IN_PROGRESS,
    XACT_COMMITTED, 
    XACT_ABORTED
} transaction_state_t;

/**
 * Transaction information used during recovery
 */
typedef struct {
    uint32_t xid;                 /* Transaction ID */
    transaction_state_t state;    /* Current transaction state */
    wal_location_t first_record;  /* First record of this transaction */
    wal_location_t last_record;   /* Last seen record of this transaction */
} transaction_info_t;

/**
 * Recovery statistics and progress information
 */
typedef struct {
    uint32_t processed_segments;          /* Number of WAL segments processed */
    uint32_t processed_records;           /* Number of WAL records processed */
    uint32_t applied_records;             /* Number of records actually applied */
    uint32_t skipped_records;             /* Number of records in incomplete transactions */
    uint32_t committed_transactions;      /* Number of committed transactions */
    uint32_t aborted_transactions;        /* Number of aborted transactions */
    uint32_t incomplete_transactions;     /* Number of incomplete transactions */
    uint64_t bytes_processed;             /* Total bytes of WAL processed */
    uint64_t recovery_time_ms;            /* Time spent in recovery (milliseconds) */
} wal_recovery_stats_t;

/**
 * Recovery context passed to callbacks
 */
typedef struct {
    void* db_instance;              /* Database instance to apply records to */
    wal_recovery_stats_t stats;     /* Recovery statistics */
    bool verbose;                   /* Whether to output verbose information */
} wal_recovery_context_t;

/**
 * Callback function to handle a specific record type during recovery
 */
typedef bool (*record_handler_t)(void* db_instance, 
                                wal_record_header_t* header, 
                                void* data, 
                                uint16_t data_len);

/**
 * Recovery handler structure for different record types
 */
typedef struct {
    record_handler_t handlers[WAL_RECORD_SCHEMA + 1];  /* Array of handlers indexed by record type */
} wal_recovery_handlers_t;

/**
 * WAL manager context 
 */
typedef struct wal_context_t wal_context_t;

/**
 * Initialize the WAL subsystem
 *
 * @param wal_dir Directory where WAL files will be stored
 * @param segment_size Size of each WAL segment file in bytes
 * @return WAL context or NULL on error
 */
wal_context_t* wal_init(const char* wal_dir, uint32_t segment_size);

/**
 * Shut down the WAL subsystem
 *
 * @param ctx WAL context to shutdown
 */
void wal_shutdown(wal_context_t* ctx);

/**
 * Begin writing a WAL record
 *
 * @param ctx WAL context
 * @param type Record type
 * @param xid Transaction ID
 * @param data_len Length of data that will follow
 * @return Pointer to buffer where caller should write data, or NULL on error
 */
void* wal_begin_record(wal_context_t* ctx, wal_record_type_t type, uint32_t xid, uint16_t data_len);

/**
 * Finish writing a WAL record
 *
 * @param ctx WAL context
 * @param location If not NULL, the WAL location of the record is stored here
 * @return true on success, false on failure
 */
bool wal_end_record(wal_context_t* ctx, wal_location_t* location);

/**
 * Force all WAL records to stable storage
 *
 * @param ctx WAL context
 * @param wait_for_sync If true, waits for sync confirmation
 * @return true on success, false on failure
 */
bool wal_flush(wal_context_t* ctx, bool wait_for_sync);

/**
 * Create a checkpoint in the WAL
 *
 * @param ctx WAL context
 * @return true on success, false on failure
 */
bool wal_checkpoint(wal_context_t* ctx);

/**
 * Read a specific WAL record
 *
 * @param ctx WAL context
 * @param location WAL location to read from
 * @param header If not NULL, header will be copied here
 * @param data If not NULL, data will be copied here
 * @param data_buf_size Size of the data buffer
 * @return true on success, false on failure
 */
bool wal_read_record(wal_context_t* ctx, wal_location_t location,
                    wal_record_header_t* header, void* data, uint16_t data_buf_size);

/**
 * Perform detailed recovery with custom record handlers
 *
 * @param ctx WAL context
 * @param end_location The location to recover up to, or {0,0} for full recovery
 * @param apply_record_callback Callback function to apply each record
 * @param user_data User data passed to the callback
 * @return true on success, false on failure
 */
bool wal_perform_recovery(wal_context_t* ctx, wal_location_t end_location, 
                         bool (*apply_record_callback)(wal_record_header_t* header, 
                                                     void* data, 
                                                     void* user_data),
                         void* user_data);

/**
 * Recover database to a consistent state after crash
 *
 * @param ctx WAL context
 * @param end_location The location to recover up to, or {0,0} for full recovery
 * @return true on success, false on failure
 */
bool wal_recover(wal_context_t* ctx, wal_location_t end_location);