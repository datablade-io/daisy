set(SRCS)


add_executable(parser_server parser_server.cpp ${SRCS})
target_link_libraries(parser_server PRIVATE clickhouse_parsers antlr4-runtime)

if (ENABLE_FUZZING)
    add_executable(parser_server_fuzzer parser_server_fuzzer.cpp ${SRCS})
    target_link_libraries(parser_server_fuzzer PRIVATE clickhouse_parsers ${LIB_FUZZING_ENGINE})
endif ()
