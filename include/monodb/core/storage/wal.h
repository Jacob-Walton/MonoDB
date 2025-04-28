/**
 * @file wal.h
 * @brief Write-Ahead Logging (WAL) system for MonoDB.
 *
 * This module implements a journaling system to ensure
 * data integrity and durability in the event of a crash.
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

/**
 * WAL record types
 */
typedef enum {
    WAL_RECORD_NULL        = 0, /* Invalid/placeholder record */
    WAL_RECORD_CHECKPOINT  = 1, /* Checkpoint record */
    WAL_RECORD_XACT_COMMIT = 2, /* Transaction commit record */
    WAL_RECORD_XACT_ABORT  = 3, /* Transaction abort record */
    WAL_RECORD_INSERT      = 4, /* Row insertion */
    WAL_RECORD_UPDATE      = 5, /* Row update */
    WAL_RECORD_DELETE      = 6, /* Row deletion */
    WAL_RECORD_NEWPAGE     = 7, /* New page allocation */
    WAL_RECORD_SCHEMA      = 8, /* Schema change */
} wal_record_type_t;

/**
 * WAL segment states
 */
typedef enum {
    WAL_SEGMENT_EMPTY    = 0,
    WAL_SEGMENT_ACTIVE   = 1,
    WAL_SEGMENT_FULL     = 2,
    WAL_SEGMENT_ARCHIVED = 3
} wal_segment_state_t;

/**
 * WAL location - comparable, sortable location identifier
 */
typedef struct {
    uint32_t segment; /* WAL segment number */
    uint32_t offset;  /* Byte offset within the segment */
} wal_location_t;

/**
 * WAL record header
 */
typedef struct {
    uint32_t          total_len;   /* Total length of record including header */
    wal_record_type_t type;        /* Type of record */
    uint32_t          xid;         /* Transaction ID (if applicable) */
    wal_location_t    prev_record; /* Location of previous record */
    uint16_t          data_len;    /* Length of data payload */
} wal_record_header_t;

/**
 * WAL manager context
 */
typedef struct wal_context_t wal_context_t;

/**
 * Initialize the WAL subsystem.
 *
 * @param wal_dir Directory where WAL files will be stored
 * @param segment_size Size of each WAL segment file in bytes
 * @return WAL context or NULL on error
 */
wal_context_t* wal_init(const char* wal_dir, uint32_t segment_size);

/**
 * Shut down the WAL subsystem.
 *
 * @param ctx WAL context to shut down
 */
void wal_shutdown(wal_context_t* ctx);

/**
 * Begin writing a new WAL record
 *
 * @param ctx WAL context
 * @param type Type of record to write
 * @param xid Transaction ID (if applicable)
 * @param data_len Length of data that will follow
 * @return Pointer to buffer where called should write data, or NULL on error
 */
void* wal_begin_record(wal_context_t* ctx, wal_record_type_t type, uint32_t xid, uint16_t data_len);

/**
 * Finish writing a WAL record
 *
 * @param ctx WAL context
 * @param location If not NULL, the WAL location of the record is here
 * @return true on success, false on error
 */
bool wal_end_record(wal_context_t* ctx, wal_location_t* location);

/**
 * Force all WAL records to stable storage
 *
 * @param ctx WAL context
 * @param wait_for_sync If true, waits for sync confirmation
 * @return true on success, false on error
 */
bool wal_flush(wal_context_t* ctx, bool wait_for_sync);

/**
 * Create a checkpoint in the WAL
 *
 * @param ctx WAL context
 * @return true on success, false on error
 */
bool wal_checkpoint(wal_context_t* ctx);

/**
 * Read a specific WAL record
 *
 * @param ctx WAL context
 * @param location Location of the record to read
 * @param header If not NULL, header will be copied here
 * @param data If not NULL, data will be copied here
 * @param data_buf_size Size of the data buffer
 * @return true on success, false on error
 */
bool wal_read_record(wal_context_t* ctx, wal_location_t location, wal_record_header_t* header,
                     void* data, uint16_t data_buf_size);

/**
 * Recover database to a consistent state using WAL records
 *
 * @param ctx WAL context
 * @param end_location The location to recover up to, or {0, 0} for full recovery
 * @return true on success, false on error
 */
bool wal_recover(wal_context_t* ctx, wal_location_t end_location);