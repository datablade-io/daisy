#include "StorageMaterializePostgreSQL.h"

#if USE_LIBPQXX
#include <Common/Macros.h>
#include <Core/Settings.h>
#include <Common/parseAddress.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>
#include <Storages/ReadFinalForExternalReplicaStorage.h>
#include <Core/PostgreSQL/PostgreSQLConnectionPool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static const auto NESTED_TABLE_SUFFIX = "_nested";


StorageMaterializePostgreSQL::StorageMaterializePostgreSQL(
    const StorageID & table_id_,
    const String & remote_database_name,
    const String & remote_table_name_,
    const postgres::ConnectionInfo & connection_info,
    const StorageInMemoryMetadata & storage_metadata,
    ContextPtr context_,
    std::unique_ptr<MaterializePostgreSQLSettings> replication_settings_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_name(remote_table_name_)
    , replication_settings(std::move(replication_settings_))
    , is_materialize_postgresql_database(
            DatabaseCatalog::instance().getDatabase(getStorageID().database_name)->getEngineName() == "MaterializePostgreSQL")
    , nested_table_id(StorageID(table_id_.database_name, getNestedTableName()))
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
{
    if (table_id_.uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MaterializePostgreSQL is allowed only for Atomic database");

    setInMemoryMetadata(storage_metadata);

    auto metadata_path = DatabaseCatalog::instance().getDatabase(getStorageID().database_name)->getMetadataPath()
                       +  "/.metadata_" + table_id_.database_name + "_" + table_id_.table_name;

    replication_handler = std::make_unique<PostgreSQLReplicationHandler>(
            remote_database_name,
            connection_info,
            metadata_path,
            getContext(),
            replication_settings->postgresql_replica_max_block_size.value,
            replication_settings->postgresql_replica_allow_minimal_ddl.value, false);
}


StorageMaterializePostgreSQL::StorageMaterializePostgreSQL(
    const StorageID & table_id_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , is_materialize_postgresql_database(true)
    , nested_table_id(table_id_)
    , nested_context(makeNestedTableContext(context_->getGlobalContext()))
{
}


StoragePtr StorageMaterializePostgreSQL::getNested() const
{
    return DatabaseCatalog::instance().getTable(nested_table_id, nested_context);
}


StoragePtr StorageMaterializePostgreSQL::tryGetNested() const
{
    return DatabaseCatalog::instance().tryGetTable(nested_table_id, nested_context);
}


std::string StorageMaterializePostgreSQL::getNestedTableName() const
{
    auto table_id = getStorageID();

    if (is_materialize_postgresql_database)
        return table_id.table_name;

    return toString(table_id.uuid) + NESTED_TABLE_SUFFIX;
}


void StorageMaterializePostgreSQL::setStorageMetadata()
{
    /// If it is a MaterializePostgreSQL database engine, then storage with engine MaterializePostgreSQL
    /// gets its metadata when it is fetch from postges, but if inner tables exist (i.e. it is a server restart)
    /// then metadata for storage needs to be set from inner table metadata.
    auto nested_table = getNested();
    auto storage_metadata = nested_table->getInMemoryMetadataPtr();
    setInMemoryMetadata(*storage_metadata);
}


std::shared_ptr<ASTColumnDeclaration> StorageMaterializePostgreSQL::getMaterializedColumnsDeclaration(
        const String name, const String type, UInt64 default_value)
{
    auto column_declaration = std::make_shared<ASTColumnDeclaration>();

    column_declaration->name = name;
    column_declaration->type = makeASTFunction(type);

    column_declaration->default_specifier = "MATERIALIZED";
    column_declaration->default_expression = std::make_shared<ASTLiteral>(default_value);

    column_declaration->children.emplace_back(column_declaration->type);
    column_declaration->children.emplace_back(column_declaration->default_expression);

    return column_declaration;
}


ASTPtr StorageMaterializePostgreSQL::getColumnDeclaration(const DataTypePtr & data_type) const
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    /// getName() for decimal returns 'Decimal(precision, scale)', will get an error with it
    if (which.isDecimal())
    {
        auto make_decimal_expression = [&](std::string type_name)
        {
            auto ast_expression = std::make_shared<ASTFunction>();

            ast_expression->name = type_name;
            ast_expression->arguments = std::make_shared<ASTExpressionList>();
            ast_expression->arguments->children.emplace_back(std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));

            return ast_expression;
        };

        if (which.isDecimal32())
            return make_decimal_expression("Decimal32");

        if (which.isDecimal64())
            return make_decimal_expression("Decimal64");

        if (which.isDecimal128())
            return make_decimal_expression("Decimal128");

        if (which.isDecimal256())
            return make_decimal_expression("Decimal256");
    }

    return std::make_shared<ASTIdentifier>(data_type->getName());
}


/// For single storage MaterializePostgreSQL get columns and primary key columns from storage definition.
/// For database engine MaterializePostgreSQL get columns and primary key columns by fetching from PostgreSQL, also using the same
/// transaction with snapshot, which is used for initial tables dump.
ASTPtr StorageMaterializePostgreSQL::getCreateNestedTableQuery(PostgreSQLTableStructurePtr table_structure)
{
    auto create_table_query = std::make_shared<ASTCreateQuery>();

    auto table_id = getStorageID();
    create_table_query->table = getNestedTableName();
    create_table_query->database = table_id.database_name;

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();
    auto order_by_expression = std::make_shared<ASTFunction>();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & columns = metadata_snapshot->getColumns();
    NamesAndTypesList ordinary_columns_and_types;

    if (!is_materialize_postgresql_database)
    {
        ordinary_columns_and_types = columns.getOrdinary();
    }
    else
    {
        if (!table_structure)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No table structure returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        if (!table_structure->columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "No columns returned for table {}.{}", table_id.database_name, table_id.table_name);
        }

        StorageInMemoryMetadata storage_metadata;

        ordinary_columns_and_types = *table_structure->columns;
        storage_metadata.setColumns(ColumnsDescription(ordinary_columns_and_types));
        setInMemoryMetadata(storage_metadata);

        if (!table_structure->primary_key_columns && !table_structure->replica_identity_columns)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table {}.{} has no primary key and no replica identity index", table_id.database_name, table_id.table_name);
        }

        NamesAndTypesList merging_columns;
        if (table_structure->primary_key_columns)
            merging_columns = *table_structure->primary_key_columns;
        else
            merging_columns = *table_structure->replica_identity_columns;

        order_by_expression->name = "tuple";
        order_by_expression->arguments = std::make_shared<ASTExpressionList>();

        for (const auto & column : merging_columns)
            order_by_expression->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
    }

    for (const auto & [name, type] : ordinary_columns_and_types)
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();

        column_declaration->name = name;
        column_declaration->type = getColumnDeclaration(type);

        columns_expression_list->children.emplace_back(column_declaration);
    }

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);

    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_sign", "Int8", 1));
    columns_declare_list->columns->children.emplace_back(getMaterializedColumnsDeclaration("_version", "UInt64", 1));

    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    /// Not nullptr for single storage (because throws exception if not specified), nullptr otherwise.
    auto primary_key_ast = getInMemoryMetadataPtr()->getPrimaryKeyAST();

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("ReplacingMergeTree", std::make_shared<ASTIdentifier>("_version")));

    if (primary_key_ast)
        storage->set(storage->order_by, primary_key_ast);
    else
        storage->set(storage->order_by, order_by_expression);

    create_table_query->set(create_table_query->storage, storage);

    return create_table_query;
}


void StorageMaterializePostgreSQL::createNestedIfNeeded(PostgreSQLTableStructurePtr table_structure)
{
    const auto ast_create = getCreateNestedTableQuery(std::move(table_structure));

    try
    {
        InterpreterCreateQuery interpreter(ast_create, nested_context);
        interpreter.execute();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


std::shared_ptr<Context> StorageMaterializePostgreSQL::makeNestedTableContext(ContextPtr from_context)
{
    auto new_context = Context::createCopy(from_context);
    new_context->makeQueryContext();
    new_context->addQueryFactoriesInfo(Context::QueryLogFactories::Storage, "ReplacingMergeTree");

    return new_context;
}


void StorageMaterializePostgreSQL::startup()
{
    if (!is_materialize_postgresql_database)
    {
        replication_handler->addStorage(remote_table_name, this);
        replication_handler->startup();
    }
}


void StorageMaterializePostgreSQL::shutdown()
{
    if (replication_handler)
        replication_handler->shutdown();
}


void StorageMaterializePostgreSQL::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (replication_handler)
        replication_handler->shutdownFinal();

    auto nested_table = getNested();
    if (nested_table && !is_materialize_postgresql_database)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, nested_table_id, no_delay);
}


NamesAndTypesList StorageMaterializePostgreSQL::getVirtuals() const
{
    return NamesAndTypesList{
            {"_sign", std::make_shared<DataTypeInt8>()},
            {"_version", std::make_shared<DataTypeUInt64>()}
    };
}


Pipe StorageMaterializePostgreSQL::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
{
    if (!nested_loaded)
        return Pipe();

    auto nested_table = getNested();

    return readFinalFromNestedStorage(
            nested_table,
            column_names,
            metadata_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
}


void registerStorageMaterializePostgreSQL(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        bool has_settings = args.storage_def->settings;
        auto postgresql_replication_settings = std::make_unique<MaterializePostgreSQLSettings>();

        if (has_settings)
            postgresql_replication_settings->loadFromQuery(*args.storage_def);

        if (engine_args.size() != 5)
            throw Exception("Storage MaterializePostgreSQL requires 5 parameters: "
                            "PostgreSQL('host:port', 'database', 'table', 'username', 'password'",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getContext());

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);

        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception("Storage MaterializePostgreSQL needs order by key or primary key", ErrorCodes::BAD_ARGUMENTS);

        if (args.storage_def->primary_key)
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        else
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext());

        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
        const String & remote_table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        /// No connection is made here, see Storages/PostgreSQL/PostgreSQLConnection.cpp
        auto connection_info = postgres::formatConnectionString(
            remote_database,
            parsed_host_port.first,
            parsed_host_port.second,
            engine_args[3]->as<ASTLiteral &>().value.safeGet<String>(),
            engine_args[4]->as<ASTLiteral &>().value.safeGet<String>());

        return StorageMaterializePostgreSQL::create(
                args.table_id, remote_database, remote_table, connection_info,
                metadata, args.getContext(),
                std::move(postgresql_replication_settings));
    };

    factory.registerStorage(
            "MaterializePostgreSQL",
            creator_fn,
            StorageFactory::StorageFeatures{
                .supports_settings = true,
                .supports_sort_order = true,
                .source_access_type = AccessType::POSTGRES,
    });
}

}

#endif
