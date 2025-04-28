# Test source files
set(TEST_WAL_SOURCES
    test_wal.c
)

# Define WAL core source files needed for tests
set(WAL_CORE_SOURCES
    ${CMAKE_SOURCE_DIR}/src/core/storage/wal.c
)

# Build the WAL test executable
add_executable(test_wal ${TEST_WAL_SOURCES} ${WAL_CORE_SOURCES})
target_include_directories(test_wal PUBLIC ${CMAKE_SOURCE_DIR}/include)

# For multi-configuration builds (VS, Xcode), specify where to find the executable
if(CMAKE_CONFIGURATION_TYPES)
    set(TEST_PATH "$<TARGET_FILE:test_wal>")
else()
    set(TEST_PATH test_wal)
endif()

# Register the test with CTest
add_test(
    NAME WAL_Test
    COMMAND ${TEST_PATH}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# Set test properties
set_tests_properties(WAL_Test PROPERTIES
    ENVIRONMENT "PATH=${CMAKE_BINARY_DIR};$ENV{PATH}"
)

message(STATUS "WAL tests configured.")
message(STATUS "To run tests manually:")
message(STATUS "  - In multi-config builds: ctest -C Debug")
message(STATUS "  - In single-config builds: ctest")