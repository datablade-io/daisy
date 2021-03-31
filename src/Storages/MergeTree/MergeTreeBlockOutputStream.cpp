#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/SequenceInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


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

        SequenceInfoPtr seq_info;

        if (hasSeqs())
        {
            seq_info = std::make_shared<SequenceInfo>(start_end_seqs.first, start_end_seqs.second, part_index, parts);
        }

        MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, metadata_snapshot, optimize_on_insert, seq_info);
        part_index++;
        /// Daisy : ends

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!part)
            continue;

        storage.renameTempPartAndAdd(part, &storage.increment);

        PartLog::addNewPart(storage.global_context, part, watch.elapsed());

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        storage.background_executor.triggerTask();
    }
}

}
