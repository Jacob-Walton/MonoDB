add_executable(nsql_repl src/main.cpp)
target_link_libraries(nsql_repl PRIVATE nsql)
target_include_directories(nsql_repl PRIVATE ${CMAKE_SOURCE_DIR}/include)

if(MSVC)
    target_compile_options(nsql_repl PRIVATE
        $<$<CONFIG:Release>:/O2>
        /W4 /permissive-
    )
else()
    target_compile_options(nsql_repl PRIVATE -Wall -Wextra -pedantic -O3)
endif()
