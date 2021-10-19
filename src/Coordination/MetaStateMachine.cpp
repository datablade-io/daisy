#include <Coordination/MetaStateMachine.h>

#include <Coordination/KVRequest.h>
#include <Coordination/MetaSnapshotManager.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>

#include <IO/ReadHelpers.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include <future>

namespace DB
{
[[maybe_unused]] std::string META_LOG_INDEX_KEY = "metastore_log_index";

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ROCKSDB_ERROR;
}

namespace
{
    Coordination::KVRequestPtr parseKVRequest(nuraft::buffer & data)
    {
        using namespace Coordination;

        ReadBufferFromNuraftBuffer buffer(data);

        KVRequestPtr request = KVRequest::read(buffer);
        return request;
    }
}

MetaStateMachine::MetaStateMachine(
    SnapshotsQueue & snapshots_queue_,
    const std::string & snapshots_path_,
    const std::string & db_path_,
    const CoordinationSettingsPtr & coordination_settings_)
    : rocksdb_dir(db_path_)
    , coordination_settings(coordination_settings_)
    , snapshot_manager(snapshots_path_, coordination_settings->snapshots_to_keep)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("MetaStateMachine"))
{
}

void MetaStateMachine::init()
{
    initDB();

    /// Do everything without mutexes, no other threads exist.
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());

    //    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    std::string result;
    rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), META_LOG_INDEX_KEY, &result);
    if (status.ok())
    {
        //        throw Exception("RocksDB load error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        last_committed_idx = std::stoi(result);
    }
    LOG_DEBUG(log, "last committed log index {}", last_committed_idx);
    while (snapshot_manager.totalSnapshots() != 0)
    {
        auto latest_log_idx = snapshot_manager.getLatestSnapshotIndex();
        try
        {
            latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_idx);
            latest_snapshot_meta = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);
            LOG_DEBUG(
                log,
                "Trying to load snapshot_meta info from snapshot up to log index {}",
                latest_snapshot_meta->snapshot_meta->get_last_log_idx());
            break;
        }
        catch (const DB::Exception & ex)
        {
            LOG_WARNING(
                log,
                "Failed to load from snapshot with index {}, with error {}, will remove it from disk",
                latest_log_idx,
                ex.displayText());
            snapshot_manager.removeSnapshot(latest_log_idx);
        }
    }
}

nuraft::ptr<nuraft::buffer> MetaStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    auto request = parseKVRequest(data);

    LOG_DEBUG(log, "commit index {}", log_idx);

    rocksdb::WriteBatch batch;
    rocksdb::Status status;

    Coordination::KVOpNum op_num = request->getOpNum();
    if (op_num != Coordination::KVOpNum::PUT)
        return nullptr;

    auto req = dynamic_cast<Coordination::KVPutRequest &>(*request);
    status = batch.Put(req.key, req.value);
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    status = batch.Put(META_LOG_INDEX_KEY, std::to_string(log_idx));
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    last_committed_idx = log_idx;

    return nullptr;
}

bool MetaStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    {
        std::lock_guard lock(snapshots_lock);
        uint64_t latest_idx = latest_snapshot_meta->snapshot_meta->get_last_log_idx();
        if (s.get_last_log_idx() != latest_idx)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Required to apply snapshot with last log index {}, but our last log index is {}",
                s.get_last_log_idx(),
                latest_idx);
        latest_snapshot_ptr = latest_snapshot_buf;
    }

    {
        std::lock_guard lock(storage_lock);
        rocksdb_ptr.reset();
        snapshot_manager.restoreFromSnapshot(rocksdb_dir, s.get_last_log_idx());
        initDB();
    }
    last_committed_idx = s.get_last_log_idx();
    return true;
}

nuraft::ptr<nuraft::snapshot> MetaStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    return latest_snapshot_meta ? latest_snapshot_meta->snapshot_meta : nullptr;
}

void MetaStateMachine::create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    auto snapshot_meta_copy = nuraft::snapshot::deserialize(*snp_buf);
    CreateSnapshotTask snapshot_task;
    {
        std::lock_guard lock(storage_lock);
        snapshot_task.snapshot = std::make_shared<MetaStorageSnapshot>(snapshot_meta_copy);
    }

    snapshot_task.create_snapshot = [this, when_done](MetaStorageSnapshotPtr && snapshot) {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            {
                std::lock_guard lock(snapshots_lock);
                latest_snapshot_buf = snapshot_manager.serializeSnapshotBufferToDisk(rocksdb_ptr.get(), *snapshot);
                latest_snapshot_meta = snapshot;

                LOG_DEBUG(log, "Created persistent snapshot {} ", latest_snapshot_meta->snapshot_meta->get_last_log_idx());
            }
        }
        catch (...)
        {
            LOG_TRACE(log, "Exception happened during snapshot");
            tryLogCurrentException(log);
            ret = false;
        }

        when_done(ret, exception);
    };

    LOG_DEBUG(log, "In memory snapshot {} created, queueing task to flash to disk", s.get_last_log_idx());
    snapshots_queue.push(std::move(snapshot_task));
}

void MetaStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    nuraft::ptr<nuraft::buffer> cloned_buffer;

    cloned_buffer = nuraft::buffer::clone(data);

    if (obj_id == 0 && is_first_obj)
    {
        std::lock_guard lock(storage_lock);

        // TODO: use another temporary variable to store MetaSnapshot before the transfer of backup
        //  files complete, to avoid use invalid on-going snapshot
        latest_snapshot_buf = cloned_buffer;
        latest_snapshot_meta = snapshot_manager.deserializeSnapshotFromBuffer(cloned_buffer);
    }

    try
    {
        std::lock_guard lock(snapshots_lock);
        snapshot_manager.saveBackupFileOfSnapshot(*latest_snapshot_meta, obj_id, *cloned_buffer);
        LOG_DEBUG(log, "Saved #{} of snapshot {} to path {}", obj_id, s.get_last_log_idx(), latest_snapshot_meta->files[obj_id]);
        obj_id++;

        if (is_last_obj)
        {
            snapshot_manager.finalizeSnapshot(s.get_last_log_idx());
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

int MetaStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s, void *& /*user_snp_ctx*/, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj)
{
    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);
    if (obj_id == 0)
    {
        data_out = nuraft::buffer::clone(*latest_snapshot_buf);
        is_last_obj = false;
    }
    else
    {
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->snapshot_meta->get_last_log_idx())
        {
            LOG_WARNING(
                log,
                "Required to apply snapshot with last log index {}, but our last log index is {}. Will ignore this one and retry",
                s.get_last_log_idx(),
                latest_snapshot_meta->snapshot_meta->get_last_log_idx());
            return -1;
        }
        data_out = snapshot_manager.loadBackupFileOfSnapshot(*latest_snapshot_meta, obj_id);
        is_last_obj = (obj_id == latest_snapshot_meta->files.size() - 1);
    }
    return 1;
}

void MetaStateMachine::processReadRequest(const KeeperStorage::RequestForSession & /*request_for_session*/)
{
    KeeperStorage::ResponsesForSessions responses;
    {
        std::lock_guard lock(storage_lock);
        //        auto status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
        //        responses = storage->processRequest(request_for_session.request, request_for_session.session_id, std::nullopt);
    }

    //    for (const auto & response : responses)
    //        responses_queue.push(response);
}

void MetaStateMachine::getByKey(const std::string & key, std::string & value)
{
    rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok() && !status.IsNotFound())
        throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

void MetaStateMachine::shutdownStorage()
{
    //    std::lock_guard lock(storage_lock);
    //    storage->finalize();
}

void MetaStateMachine::initDB()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " + rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}

}
