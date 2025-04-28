/**
 * @file test_wal.c
 * @brief Tests for the Write-Ahead Log system
 */

#include <monodb/core/storage/wal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char* argv[]) {
    /* Mark unused parameters */
    (void)argc;
    (void)argv;

    printf("MonoDB WAL Test - Starting up...\n");

    /* WAL configuration */
    const char* wal_dir      = "./test_wal";
    uint32_t    segment_size = 16 * 1024 * 1024; /* 16MB segment size */

    printf("Initializing WAL system in directory: %s\n", wal_dir);
    wal_context_t* wal = wal_init(wal_dir, segment_size);

    if (!wal) {
        fprintf(stderr, "Failed to initialize WAL system\n");
        return 1;
    }

    printf("WAL system initialized successfully\n");

    /* Demonstrate recovery with sample transactions */
    /* Transaction 1: Commit */
    uint32_t xid1 = 1001;

    printf("\n--- Transaction %u (COMMIT) ---\n", xid1);

    /* Begin transaction record */
    void* data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_COMMIT, xid1, 0);
    if (!data_ptr || !wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to write transaction begin record\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Insert record (using NSQL syntax for data) */
    char insert_data[] =
        "TELL users TO ADD RECORD WITH id = 1, name = 'John Doe', email = 'john@example.com'";
    size_t data_len = strlen(insert_data) + 1;

    /* Cast size_t to uint16_t for wal_begin_record's length parameter */
    data_ptr = wal_begin_record(wal, WAL_RECORD_INSERT, xid1, (uint16_t)data_len);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    memcpy(data_ptr, insert_data, data_len);

    if (!wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to end WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Added INSERT record: %s\n", insert_data);

    /* Update record (using NSQL syntax for data) */
    char update_data[] = "TELL users TO UPDATE name = 'John Smith' WHERE id = 1";
    data_len           = strlen(update_data) + 1;

    /* Cast size_t to uint16_t */
    data_ptr = wal_begin_record(wal, WAL_RECORD_UPDATE, xid1, (uint16_t)data_len);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    memcpy(data_ptr, update_data, data_len);

    if (!wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to end WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Added UPDATE record: %s\n", update_data);

    /* Commit transaction record */
    data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_COMMIT, xid1, 0);
    if (!data_ptr || !wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to write transaction commit record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Transaction committed\n");

    /* Transaction 2: Abort */
    uint32_t xid2 = 1002;

    printf("\n--- Transaction %u (ABORT) ---\n", xid2);

    /* Begin transaction record */
    data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_COMMIT, xid2, 0);
    if (!data_ptr || !wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to write transaction begin record\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Delete record (using NSQL syntax for data) */
    char delete_data[] = "TELL users TO REMOVE WHERE id = 1";
    data_len           = strlen(delete_data) + 1;

    /* Cast size_t to uint16_t */
    data_ptr = wal_begin_record(wal, WAL_RECORD_DELETE, xid2, (uint16_t)data_len);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    memcpy(data_ptr, delete_data, data_len);

    if (!wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to end WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Added DELETE record: %s\n", delete_data);

    /* Abort transaction record */
    data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_ABORT, xid2, 0);
    if (!data_ptr || !wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to write transaction abort record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Transaction aborted\n");

    /* Transaction 3: Incomplete (simulates crash) */
    uint32_t xid3 = 1003;

    printf("\n--- Transaction %u (INCOMPLETE) ---\n", xid3);

    /* Begin transaction record */
    data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_COMMIT, xid3, 0);
    if (!data_ptr || !wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to write transaction begin record\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Schema change record (using NSQL syntax for data) */
    char schema_data[] = "TELL users TO ADD email_verified AS BOOLEAN DEFAULT FALSE";
    data_len           = strlen(schema_data) + 1;

    /* Cast size_t to uint16_t */
    data_ptr = wal_begin_record(wal, WAL_RECORD_SCHEMA, xid3, (uint16_t)data_len);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    memcpy(data_ptr, schema_data, data_len);

    if (!wal_end_record(wal, NULL)) {
        fprintf(stderr, "Failed to end WAL record\n");
        wal_shutdown(wal);
        return 1;
    }

    printf("  Added SCHEMA record: %s\n", schema_data);

    /* Simulate crash before commit/abort */
    printf("  Transaction left incomplete (simulating crash)\n");

    /* Ensure WAL data is persisted */
    printf("\nFlushing WAL to disk\n");
    if (!wal_flush(wal, true)) { /* Force flush */
        fprintf(stderr, "Failed to flush WAL\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Create a checkpoint to establish a recovery point */
    printf("Creating checkpoint\n");
    if (!wal_checkpoint(wal)) {
        fprintf(stderr, "Failed to create checkpoint\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Cleanly shut down the WAL system */
    printf("Shutting down WAL system\n");
    wal_shutdown(wal);

    /* Simulate database restart and recovery */
    printf("\n========= DATABASE RESTART =========\n");

    /* Re-initialize WAL for recovery */
    printf("Initializing WAL system for recovery\n");
    wal = wal_init(wal_dir, segment_size);
    if (!wal) {
        fprintf(stderr, "Failed to reopen WAL system\n");
        return 1;
    }

    /* Perform recovery from the WAL */
    printf("\nStarting WAL recovery process...\n");
    /* Recover all records up to the latest */
    if (!wal_recover(wal, (wal_location_t){0, 0})) {
        fprintf(stderr, "Recovery failed\n");
        wal_shutdown(wal);
        return 1;
    }

    /* Final shutdown */
    wal_shutdown(wal);

    printf("\nWAL test completed successfully\n");
    return 0;
}