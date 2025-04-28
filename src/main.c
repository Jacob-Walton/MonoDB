#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <monodb/core/storage/wal.h>

int main(int argc, char* argv[]) {
    printf("MonoDB - Starting...\n");

    /* Initialize WAL system */
    const char* wal_dir = "./wal";
    uint32_t   segment_size = 16 * 1024 * 1024; /* 16MB */

    printf("Initializing WAL system in directory: %s\n", wal_dir);
    wal_context_t* wal = wal_init(wal_dir, segment_size);

    if (!wal) {
        fprintf(stderr, "Failed to initialize WAL system\n");
        return EXIT_FAILURE;
    }

    printf("WAL system initialized successfully\n");

    /* Create a sample transaction with multiple operations */
    uint32_t xid = 1001; /* Simple ID*/

    /* Transaction begin record */
    printf("Starting transaction with ID: %u\n", xid);
    void* data_ptr = wal_begin_record(wal, WAL_RECORD_XACT_COMMIT, xid, 0);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin transaction record\n");
        wal_shutdown(wal);
        return EXIT_FAILURE;
    }

    wal_location_t location;
    if (!wal_end_record(wal, &location)) {
        fprintf(stderr, "Failed to end transaction record\n");
        wal_shutdown(wal);
        return EXIT_FAILURE;
    }

    printf("Transaction begin record written at segment %u, offset %u\n",
           location.segment, location.offset);

    /* Insert record */
    printf("Writing insert record\n");
    char insert_data[] = "TELL users TO ADD RECORD WITH id = 1, name = 'John Doe', email = 'john@example.com'";
    size_t data_len = strlen(insert_data) + 1; /* +1 for null terminator */

    data_ptr = wal_begin_record(wal, WAL_RECORD_INSERT, xid, data_len);
    if (!data_ptr) {
        fprintf(stderr, "Failed to begin insert record\n");
        wal_shutdown(wal);
        return EXIT_FAILURE;
    }

    /* Copy data to WAL record */
    memcpy(data_ptr, insert_data, data_len);

    if (!wal_end_record(wal, &location)) {
        fprintf(stderr, "Failed to end insert record\n");
        wal_shutdown(wal);
        return EXIT_FAILURE;
    }

    printf("Insert record written at segment %u, offset %u\n", location.segment,
           location.offset);
}