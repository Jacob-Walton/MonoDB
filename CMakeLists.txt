cmake_minimum_required(VERSION 3.15)

project(MonoDB
    VERSION 0.1.0
    DESCRIPTION "MonoDB"
    LANGUAGES C CXX
)

# Set C standard globally
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# Set C++ standard globally
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set build type to Release by default if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    message(STATUS "Setting build type to Release as none was specified")
endif()

# Source files for the library
set(MONODB_SOURCES
    src/core/storage/wal.c
    src/main.c
)

# Build MonoDB
add_executable(monodb ${MONODB_SOURCES})

# Include directories for the library
target_include_directories(monodb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

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

# Standard test target
if(TARGET test_runner OR TARGET test_lexer OR TARGET test_parser OR TARGET test_serializer)
    add_custom_target(check
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
        DEPENDS
            $<$<TARGET_EXISTS:test_runner>:test_runner>
            $<$<TARGET_EXISTS:test_lexer>:test_lexer>
            $<$<TARGET_EXISTS:test_parser>:test_parser>
            $<$<TARGET_EXISTS:test_serializer>:test_serializer>
        COMMENT "Running all tests"
    )
endif()