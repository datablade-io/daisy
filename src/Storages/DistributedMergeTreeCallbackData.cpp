#include "DistributedMergeTreeCallbackData.h"
#include "StorageDistributedMergeTree.h"

#include <common/logger_useful.h>

/// Daisy : starts. Added for Daisy

namespace DB
{
void DistributedMergeTreeCallbackData::wait() const
{
    while (outstanding_commits != 0)
    {
        LOG_INFO(storage->log, "Waiting for outstanding commits={} to finish", outstanding_commits);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void DistributedMergeTreeCallbackData::commit(IDistributedWriteAheadLog::RecordPtrs records)
{
    if (finishRecovery())
    {
        doCommit(std::move(records));
    }
    else
    {
        /// Recovery path
        /// During startup, there may have missing sequence ranges in last run
        /// We need re-compose these missing sequence ranges and commit again
        /// to avoid data lost and duplicate data. Duplicate data is enfored later
        /// after partitioning each missing sequence range

        for (auto & record : records)
        {
            recovery_records.push_back(std::move(record));
        }

        /// Wait until we consume a record which has sequence number larger
        /// than max committed sn
        if (recovery_records.back()->sn <= storage->maxCommittedSN())
        {
            return;
        }

        auto range_buckets{categorizeRecordsAccordingToSequenceRanges(recovery_records, missing_sequence_ranges, storage->maxCommittedSN())};

        for (auto & records_seqs_pair : range_buckets)
        {
            assert(!records_seqs_pair.first.empty());
            doCommit(std::move(records_seqs_pair.first), std::move(records_seqs_pair.second));
        }

        /// We have done with recovery, clean up data structure to speedup fast path condition check
        recovery_records.clear();
        missing_sequence_ranges.clear();
    }
}

inline void DistributedMergeTreeCallbackData::doCommit(IDistributedWriteAheadLog::RecordPtrs records, SequenceRanges sequence_ranges)
{
    ++outstanding_commits;
    try
    {
        storage->commit(std::move(records), std::move(sequence_ranges), ctx);
    }
    catch (...)
    {
        LOG_ERROR(
            storage->log, "Failed to commit data for shard={}, exception={}", storage->shard, getCurrentExceptionMessage(true, true));
    }
    --outstanding_commits;
}

RecordsMissngSequenceRangesPair DistributedMergeTreeCallbackData::categorizeRecordsAccordingToSequenceRanges(
    const IDistributedWriteAheadLog::RecordPtrs & records,
    const SequenceRanges & sequence_ranges,
    IDistributedWriteAheadLog::RecordSequenceNumber max_committed_sn)
{
    std::vector<RecordsMissngSequenceRangesPair> range_buckets;
    range_buckets.reserve(sequence_ranges.size() + 1);
    IDistributedWriteAheadLog::RecordPtrs new_records;

    for (auto & record : records)
    {
        if (record->sn > max_committed_sn)
        {
            new_records.push_back(std::move(record));
            continue;
        }

        if (record->sn > sequence_ranges.back().end_sn)
        {
            /// records fall in [sequence_ranges.back().end_sn, max_committed_sn] are committed
            continue;
        }

        /// Find the correponding missing sequence range for current record
        for (size_t i = 0; i < sequence_ranges.size(); ++i)
        {
            const auto & sequence_range = sequence_ranges[i];
            if (record->sn >= sequence_range.start_sn && record->sn <= sequence_range.end_sn)
            {
                /// Found the missing range for current record
                if (range_buckets.empty() || range_buckets.back().second.back().start_sn != sequence_range.start_sn)
                {
                    /// Start a new range bucket
                    range_buckets.push_back({});
                }

                auto & records_seqs_pair = range_buckets.back();
                records_seqs_pair.first.push_back(std::move(record));
                records_seqs_pair.second.push_back(sequence_range);

                /// Collect all missing sequence range parts
                for (size_t j = i + 1; j < sequence_ranges.size(); ++j)
                {
                    if (sequence_ranges[j].start_sn == sequence_range.start_sn)
                    {
                        records_seqs_pair.second.push_back(sequence_ranges[j]);
                    }
                }
                break;
            }
        }

        /// If a record doesn't belong to any missing sequence range,
        /// it means it was committed already (we have sequence range hole)
    }

    if (!new_records.empty())
    {
        range_buckets.emplace_back(std::move(new_records), {});
    }
    return range_buckets;
}
}
