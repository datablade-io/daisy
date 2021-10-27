set(CLICKHOUSE_METASTORE_SOURCES
    MetaStore.cpp
)

if (OS_LINUX)
    set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_metastore_configs> -Wl,${NO_WHOLE_ARCHIVE}")
endif ()

set (CLICKHOUSE_METASTORE_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        daemon
        dbms

    ${LINK_RESOURCE_LIB}
)

clickhouse_program_add(metastore)

install (FILES metastore_config.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-metastore" COMPONENT clickhouse-metastore)

clickhouse_embed_binaries(metastore metastore_config.xml metastore_embedded.xml)
