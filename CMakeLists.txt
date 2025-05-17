cmake_minimum_required(VERSION 3.15)

project(MonoDB
    VERSION 0.1.0
    DESCRIPTION "MonoDB"
    LANGUAGES C CXX
)

# Set C standard globally
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

# Set C++ standard globally
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

# Set build type to Release by default if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    message(STATUS "Setting build type to Release as none was specified")
endif()

# Source files for MonoDB
set(MONODB_SOURCES
    src/core/storage/wal.c
    src/core/query/processor.c
    src/main.c
)

# Build MonoDB
add_executable(monodb ${MONODB_SOURCES})

add_subdirectory(NSQL)
add_subdirectory(repl)

# Include directories for the library
target_include_directories(monodb PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/nsql/include
)

# Update includes and link libraries
target_include_directories(monodb PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${NSQL_DIR}/include
    ${SAFECLIB_INCLUDE_DIR}
)

target_link_libraries(monodb PRIVATE nsql)

# Link socket library on Windows
if(WIN32)
    target_link_libraries(monodb PRIVATE ws2_32)
endif()

# Compiler flags
if(MSVC)
    target_compile_options(monodb PRIVATE
        $<$<CONFIG:Release>:/O2>
        /W4 /permissive-
    )
else()
    target_compile_options(monodb PRIVATE -Wall -Wextra -pedantic -O3)
endif()

# Tests
option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Get current configuration for multi-config generators
if(CMAKE_CONFIGURATION_TYPES)
    # For multi-configuration builds (VS, Xcode)
    set(CMAKE_CTEST_ARGUMENTS --build-config $<CONFIG>)
else()
    # For single-configuration builds (Unix Makefiles, Ninja)
    set(CMAKE_CTEST_ARGUMENTS "")
endif()

# Standard test target
if(TARGET test_runner OR TARGET test_lexer OR TARGET test_parser OR TARGET test_serializer OR TARGET test_wal)
    add_custom_target(check
        COMMAND ${CMAKE_CTEST_COMMAND} ${CMAKE_CTEST_ARGUMENTS} --output-on-failure
        DEPENDS
            $<$<TARGET_EXISTS:test_runner>:test_runner>
            $<$<TARGET_EXISTS:test_lexer>:test_lexer>
            $<$<TARGET_EXISTS:test_parser>:test_parser>
            $<$<TARGET_EXISTS:test_serializer>:test_serializer>
            $<$<TARGET_EXISTS:test_wal>:test_wal>
        COMMENT "Running all tests"
    )
endif()
