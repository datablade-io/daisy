#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/SequenceInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <common/logger_useful.h>


namespace DB
{

Block MergeTreeBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void MergeTreeBlockOutputStream::writePrefix()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded();
}


void MergeTreeBlockOutputStream::write(const Block & block)
{
    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot);

    /// Daisy : starts
    Int32 parts = static_cast<Int32>(part_blocks.size());
    Int32 part_index = 0;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;

        if (ignorePartBlock(parts, part_index))
        {
            part_index++;
            continue;
        }

        SequenceInfoPtr part_seq;

        if (seq_info)
        {
            part_seq = seq_info->shallowClone(part_index, parts);
        }

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, part_seq, context);

        part_index++;
        /// Daisy : ends

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!part)
            continue;

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (storage.renameTempPartAndAdd(part, &storage.increment, nullptr, storage.getDeduplicationLog()))
        {
            PartLog::addNewPart(storage.getContext(), part, watch.elapsed());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_executor.triggerTask();
        }
    }
}

/// Daisy : starts
inline bool MergeTreeBlockOutputStream::ignorePartBlock(Int32 parts, Int32 part_index) const
{
    if (missing_seq_ranges.empty())
    {
        return false;
    }

    const auto & last_seq_range = missing_seq_ranges.back();
    assert(parts == last_seq_range.parts);

    if (parts != last_seq_range.parts)
    {
        /// This shall not happen. If it does happen, the table partition algorithm
        /// in table definition has been changed. We just persist the blocks in favor
        /// of avoiding data loss (there can be data duplication)
        LOG_WARNING(
            storage.log,
            "Recovery phase. Expecting parts={}, but got={} for start_sn={}, end_sn={}. Partition algorithm is probably changed",
            parts,
            last_seq_range.parts,
            last_seq_range.start_sn,
            last_seq_range.end_sn);
        return false;
    }

    /// Recovery phase, only persist missing sequence ranges to avoid duplicate data
    for (const auto & seq_range : missing_seq_ranges)
    {
        if (seq_range.part_index == part_index)
        {
            /// The part block is in missing sequence ranges, persist it
            return false;
        }
    }

    LOG_WARNING(
        storage.log,
        "Recovery phase. Skipping persisting parts={} part_index={} for start_sn={}, end_sn={} because it was previously persisted",
        parts,
        part_index,
        last_seq_range.start_sn,
        last_seq_range.end_sn);

    return true;
}
/// Daisy : ends

}
