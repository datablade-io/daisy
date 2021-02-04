#include <DataStreams/MaterializingBlockOutputStream.h>
#include <Storages/DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Storages/DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/TerminalSize.h>
#include <Common/ThreadPool.h>

#include <boost/program_options.hpp>

using namespace std;
using namespace DB;

Block prepare_data(Int32 batch_size)
{
    Block block;

    auto uint64_type = make_shared<DataTypeUInt64>();
    auto float64_type = make_shared<DataTypeFloat64>();
    auto datetime64_type = make_shared<DataTypeDateTime64>(3);
    auto string_type = make_shared<DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto id_col_inner = typeid_cast<ColumnUInt64 *>(id_col.get());
    for (Int32 i = 0; i < batch_size; ++i)
    {
        id_col_inner->insertValue(i);
    }

    ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto cpu_col_inner = typeid_cast<ColumnFloat64 *>(cpu_col.get());
    for (Int32 i = 0; i < batch_size; ++i)
    {
        cpu_col_inner->insertValue(13.338 + i);
    }

    ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    String log{"2021.01.13 03:48:02.311031 [ 4070  ] {} <Information> Application: It looks like the process has no CAP_IPC_LOCK capability, binary mlock will be disabled. It could happen due to incorrect ClickHouse package installation. You could resolve the problem manually with 'sudo setcap cap_ipc_lock=+ep /home/ghost/code/private-daisy/build/programs/clickhouse'. Note     that it will not work on 'nosuid' mounted filesystems"};
    auto raw_col = string_type->createColumn();
    for (Int32 i = 0; i < batch_size; ++i)
    {
        raw_col->insertData(log.data(), log.size());
    }

    ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto time_col_inner = typeid_cast<ColumnDecimal<DateTime64> *>(time_col.get());

    for (Int32 i = 0; i < batch_size; ++i)
    {
        time_col_inner->insertValue(1612286044.256326 + i);
    }

    ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, "_time");
    block.insert(time_col_with_type);

    return block;
}

struct BenchmarkSettings
{
    Int32 concurrency = 1;
    Int32 iterations = 1;
    Int32 batch_size = 1;
    Int32 wal_client_pool_size = 1;
    String topic = "daisy";
    unique_ptr<DistributedWriteAheadLogKafkaSettings> wal_settings;

    bool parse_result = false;
};

static BenchmarkSettings parse_args(int argc, char ** argv)
{
    using boost::program_options::value;

    boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    auto options = desc.add_options();
    options("help", "help message");
    options("concurrency", value<Int32>()->default_value(1), "number of parallel ingestion");
    options("iterations", value<Int32>()->default_value(1), "number of iterations");
    options("batch_size", value<Int32>()->default_value(100), "number of rows in one Kafka message");
    options("wal_client_pool_size", value<Int32>()->default_value(1), "WAL client pool size");

    options("enable_idempotence", value<bool>()->default_value(true), "idempotently ingest data into Kafka");
    options("request_required_acks", value<Int32>()->default_value(1), "number of acks to wait per Kafka message");
    options(
        "queue_buffering_max_messages",
        value<Int32>()->default_value(1),
        "number of message to buffer in client before sending to Kafka brokers");
    options("queue_buffering_max_ms", value<Int32>()->default_value(1), "max time to buffer message on client side before sending to Kafka brokers");
    options("socket_blocking_max_ms", value<Int32>()->default_value(1), "max time to block on socket when sending to Kafka brokers");
    options("message_delivery_poll_max_ms", value<Int32>()->default_value(500), "max time to wait for one message to deliver");
    options("compression_codec", value<String>()->default_value("snappy"), "none,gzip,snappy,lz4,zstd,inherit");
    options("kafka_brokers", value<String>()->default_value("localhost:9092"), "Kafka broker lists");
    options("kafka_topic", value<String>()->default_value("daisy"), "Kafka topic");
    options("log_level", value<Int32>()->default_value(6), "Log level");

    boost::program_options::variables_map option_map;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), option_map);
    boost::program_options::notify(option_map);

    if (option_map.count("help"))
    {
        cout << "Usage: " << argv[0] << " " << desc << "\n";
        return {};
    }

    auto settings = make_unique<DistributedWriteAheadLogKafkaSettings>();
    settings->brokers = option_map["kafka_brokers"].as<String>();
    settings->request_required_acks = option_map["request_required_acks"].as<Int32>();
    settings->queue_buffering_max_messages = option_map["queue_buffering_max_messages"].as<Int32>();
    settings->queue_buffering_max_ms = option_map["queue_buffering_max_ms"].as<Int32>();
    settings->socket_blocking_max_ms = option_map["socket_blocking_max_ms"].as<Int32>();
    settings->message_delivery_poll_max_ms = option_map["message_delivery_poll_max_ms"].as<Int32>();
    settings->log_level = option_map["log_level"].as<Int32>();
    settings->enable_idempotence = option_map["enable_idempotence"].as<bool>();
    settings->compression_codec = option_map["compression_codec"].as<String>();

    BenchmarkSettings bench_settings;

    bench_settings.wal_settings = move(settings);
    bench_settings.topic = option_map["kafka_topic"].as<String>();
    bench_settings.wal_client_pool_size = option_map["wal_client_pool_size"].as<Int32>();
    bench_settings.iterations = option_map["iterations"].as<Int32>();
    bench_settings.batch_size = option_map["batch_size"].as<Int32>();
    bench_settings.concurrency = option_map["concurrency"].as<Int32>();
    bench_settings.parse_result = true;

    return bench_settings;
}

int mainEntryClickHouseDWal(int argc, char ** argv)
{
    auto bench_settings = parse_args(argc, argv);
    if (!bench_settings.parse_result)
        return 1;

    vector<shared_ptr<DistributedWriteAheadLogKafka>> wals;
    wals.reserve(bench_settings.wal_client_pool_size);
    for (Int32 i = 0; i < bench_settings.wal_client_pool_size; ++i)
    {
        auto settings = make_unique<DistributedWriteAheadLogKafkaSettings>();
        /// make a copy
        *settings = *bench_settings.wal_settings;
        wals.push_back(make_shared<DistributedWriteAheadLogKafka>(move(settings)));
        wals.back()->startup();
    }

    ThreadPool worker_pool{static_cast<size_t>(bench_settings.concurrency)};

    mutex stdout_mtx;

    for (Int32 jobid = 0; jobid < bench_settings.concurrency; ++jobid)
    {
        worker_pool.scheduleOrThrowOnError([jobid, &wals, &bench_settings, &stdout_mtx] {
            stdout_mtx.lock();
            cout << "thread id=" << this_thread::get_id() << " got jobid=" << jobid << " and grabbed walid=" << jobid % wals.size()
                 << " to ingest data\n";
            stdout_mtx.unlock();

            auto & wal = wals[jobid % wals.size()];
            any ctx{DistributedWriteAheadLogKafkaContext{bench_settings.topic, 0}};

            for (Int32 i = 0; i < bench_settings.iterations; ++i)
            {
                IDistributedWriteAheadLog::Record record{IDistributedWriteAheadLog::ActionType::ADD_DATA_BLOCK, prepare_data(bench_settings.batch_size)};
                record.partition_key = i;
                record.idempotent_key = to_string(i);
                IDistributedWriteAheadLog::AppendResult result = wal->append(record, ctx);
                /// cout << "producing record with sequence number : " << result.sn << " (partition, partition_key)=" << any_cast<Int32>(result.ctx) << ":" << i << "\n";
            }
        });
    }

    worker_pool.wait();

    return 0;
}
