#pragma once
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>
//#include <Coordination/KeeperStorage.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <rocksdb/db.h>
#include "rocksdb/utilities/backupable_db.h"

namespace DB
{
using SnapshotMetadata = nuraft::snapshot;
using SnapshotMetadataPtr = std::shared_ptr<SnapshotMetadata>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
};

struct MetaStorageSnapshot
{
public:
    explicit MetaStorageSnapshot(uint64_t up_to_log_idx_);

    explicit MetaStorageSnapshot(const SnapshotMetadataPtr & snapshot_meta_);
    ~MetaStorageSnapshot();

    static void serialize(const MetaStorageSnapshot & snapshot, WriteBuffer & out);

    static std::shared_ptr<MetaStorageSnapshot> deserialize(ReadBuffer & in);

    //    KeeperStorage * storage;

    SnapshotVersion version = SnapshotVersion::V0;
    SnapshotMetadataPtr snapshot_meta;
    rocksdb::BackupInfo backup_info;
    std::map<uint64_t, std::string> files;
};

using MetaStorageSnapshotPtr = std::shared_ptr<MetaStorageSnapshot>;
using CreateSnapshotCallback = std::function<void(MetaStorageSnapshotPtr &&)>;

class MetaSnapshotManager
{
public:
    MetaSnapshotManager(const std::string & snapshots_path_, size_t snapshots_to_keep_);

    static nuraft::ptr<nuraft::buffer> serializeSnapshotToBuffer(const MetaStorageSnapshot & snapshot);

    nuraft::ptr<nuraft::buffer> serializeSnapshotBufferToDisk(rocksdb::DB * storage, MetaStorageSnapshot & meta);


    static MetaStorageSnapshotPtr deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer);

    nuraft::ptr<nuraft::buffer> deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const;

    nuraft::ptr<nuraft::buffer> deserializeLatestSnapshotBufferFromDisk();

    void restoreFromSnapshot(const std::string & rocksdb_dir, uint64_t up_to_log_idx) const;

    void saveBackupFileOfSnapshot(const MetaStorageSnapshot & snapshot, uint64_t obj_id, nuraft::buffer & buffer);

    nuraft::ptr<nuraft::buffer> loadBackupFileOfSnapshot(const MetaStorageSnapshot & snapshot, uint64_t obj_id) const;

    void finalizeSnapshot(uint64_t up_to_log_idx);

    void removeSnapshot(uint64_t log_idx);

    size_t totalSnapshots() const { return existing_snapshots.size(); }

    size_t getLatestSnapshotIndex() const
    {
        if (!existing_snapshots.empty())
            return existing_snapshots.rbegin()->first;
        return 0;
    }

private:
    void removeOutdatedSnapshotsIfNeeded();
    const std::string snapshots_path;
    const size_t snapshots_to_keep;
    std::map<uint64_t, std::string> existing_snapshots;

    Poco::Logger * log;
};

struct CreateSnapshotTask
{
    MetaStorageSnapshotPtr snapshot;
    CreateSnapshotCallback create_snapshot;
};

}
