#include "SequenceInfo.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
inline SequenceRange parseSequenceRange(const String & s, String::size_type lpos, String::size_type rpos)
{
    SequenceRange seq_range;

    for (Int32 i = 0; i < 3; ++i)
    {
        auto comma_pos = s.find(',', lpos);
        if (comma_pos == String::npos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        if (comma_pos > rpos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        switch (i)
        {
            case 0:
                seq_range.start_sn = parseIntStrict<Int64>(s, lpos, comma_pos);
                break;
            case 1:
                seq_range.end_sn  = parseIntStrict<Int64>(s, lpos, comma_pos);
                break;
            case 2:
                seq_range.part_index = parseIntStrict<Int32>(s, lpos, comma_pos);
                break;
        }

        lpos = comma_pos + 1;
    }

    seq_range.parts = parseIntStrict<Int32>(s, lpos, rpos);

    return seq_range;
}

SequenceRanges readSequenceRanges(ReadBuffer & in)
{
    assertString("seqs:", in);

    String data;
    DB::readText(data, in);

    if (data.empty())
    {
        return {};
    }

    SequenceRanges sequence_ranges;

    String::size_type siz = static_cast<String::size_type>(data.size());
    String::size_type lpos = 0;

    while (lpos < siz)
    {
        auto pos = data.find(';', lpos);
        if (pos == String::npos)
        {
            sequence_ranges.push_back(parseSequenceRange(data, lpos, siz));
            break;
        }
        else
        {
            sequence_ranges.push_back(parseSequenceRange(data, lpos, pos));
            lpos = pos + 1;
        }
    }

    return sequence_ranges;
}

inline IdempotentKey parseIdempotentKey(const String & s, String::size_type lpos, String::size_type rpos)
{
    IdempotentKey key;

    auto comma_pos = s.find(',', lpos);
    if (comma_pos == String::npos)
    {
        throw Exception("Invalid idempotent key" + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    if (comma_pos >= rpos)
    {
        throw Exception("Invalid idempotent key" + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    key.first = parseIntStrict<Int64>(s, lpos, comma_pos);
    key.second = String{s, comma_pos + 1, rpos - comma_pos - 1};

    return key;
}

std::shared_ptr<IdempotentKeys> readIdempotentKeys(ReadBuffer & in)
{
    assertString("keys:", in);

    String data;
    DB::readText(data, in);

    if (data.empty())
    {
        return {};
    }

    auto idempotent_keys = std::make_shared<IdempotentKeys>();

    String::size_type siz = static_cast<String::size_type>(data.size());
    String::size_type lpos = 0;

    for (; lpos < siz;)
    {
        auto pos = data.find(';', lpos);
        if (pos == String::npos)
        {
            idempotent_keys->push_back(parseIdempotentKey(data, lpos, siz));
            break;
        }
        else
        {
            idempotent_keys->push_back(parseIdempotentKey(data, lpos, pos));
            lpos = pos + 1;
        }
    }

    return idempotent_keys;
}

SequenceRanges mergeSequenceRanges(const std::vector<SequenceInfoPtr> & sequences, Int64 committed_sn, Poco::Logger * log)
{
    SequenceRanges merged;
    SequenceRange last_seq_range;

    size_t total_ranges = 0;
    Int64 min_sn = -1;
    Int64 max_sn = -1;

    for (auto & seq_info : sequences)
    {
        total_ranges += seq_info->sequence_ranges.size();

        /// Merge ranges
        for (const auto & next_seq_range : seq_info->sequence_ranges)
        {
            assert(next_seq_range.valid());

            if (min_sn == -1)
            {
                min_sn = next_seq_range.start_sn;
            }

            if (next_seq_range.end_sn > max_sn)
            {
                max_sn = next_seq_range.end_sn;
            }

            if (!last_seq_range.valid())
            {
                last_seq_range = next_seq_range;
            }
            else
            {
                /// There shall be no cases where there is sequence overlapping in a partition
                assert(last_seq_range.end_sn < next_seq_range.start_sn);
                if (last_seq_range.end_sn >= next_seq_range.start_sn)
                {
                    LOG_ERROR(
                        log,
                        "Duplicate sn found: ({}, {}) -> ({}, {})",
                        last_seq_range.start_sn,
                        last_seq_range.end_sn,
                        next_seq_range.start_sn,
                        next_seq_range.end_sn);
                }
                last_seq_range = next_seq_range;
            }

            /// We don't check sequence gap here. There are 3 possible cases
            /// 1. The missing sequence is not committed yet (rarely)
            /// 2. It is casued by resending an / several idempotent blocks which will be deduped and ignored.
            /// 3. The Block whish has the missing sequence is distributed to a different partition
            /// Only for sequence ranges which are beyond the `committed_sn`, we need merge them and
            /// keep them around
            if (next_seq_range.end_sn > committed_sn)
            {
                /// We can still merge, because `committed_sn` confirms that
                /// all blocks with sequence IDs less than it are committed
                merged.push_back(next_seq_range);
            }
        }
    }

    if (log)
    {
        LOG_DEBUG(
            log,
            "Merge {} sequence ranges to {}, committed_sn={} min_sn={} max_sn={}",
            total_ranges,
            merged.size(),
            committed_sn,
            min_sn,
            max_sn);
    }

    return merged;
}

std::shared_ptr<IdempotentKeys>
mergeIdempotentKeys(std::vector<SequenceInfoPtr> & sequences, UInt64 max_idempotent_keys, Poco::Logger * log)
{
    auto start_iter = sequences.rend();
    size_t start_pos = 0;
    size_t key_count = 0;

    for (auto iter = sequences.rbegin(); iter != sequences.rend(); ++iter)
    {
        const auto & seq_info = **iter;
        if (seq_info.idempotent_keys)
        {
            key_count += seq_info.idempotent_keys->size();

            if (key_count >= max_idempotent_keys)
            {
                if (start_pos == 0)
                {
                    start_pos = key_count - max_idempotent_keys;
                    start_iter = iter;
                }
            }
        }
    }

    if (start_iter == sequences.rend())
    {
        --start_iter;
    }

    if (key_count == 0)
    {
       return nullptr;
    }

    auto idempotent_keys = std::make_shared<IdempotentKeys>();
    for (; start_iter != sequences.rbegin(); --start_iter)
    {
        auto & seq_info = **start_iter;

        if (start_pos > 0)
        {
            /// We will need fist skipping `start_pos` elements
            size_t count = 0;
            for (const auto & key : *seq_info.idempotent_keys)
            {
                if (count++ < start_pos)
                {
                    continue;
                }
                idempotent_keys->push_back(key);
            }

            start_pos = 0;
        }
        else if (seq_info.idempotent_keys)
        {
            for (const auto & key : *seq_info.idempotent_keys)
            {
                idempotent_keys->push_back(key);
            }
        }
    }

    if ((*sequences.rbegin())->idempotent_keys)
    {
        size_t count = 0;
        for (const auto & key : *(*sequences.rbegin())->idempotent_keys)
        {
            if (count++ < start_pos)
            {
                continue;
            }
            idempotent_keys->push_back(key);
        }
    }

    if (log)
    {
        LOG_DEBUG(log, "Merge {} idempotent keys to {}, max_idempotent_keys={}", key_count, idempotent_keys->size(), max_idempotent_keys);
    }

    return idempotent_keys;
}
}

bool operator==(const SequenceRange & lhs, const SequenceRange & rhs)
{
    return lhs.start_sn == rhs.start_sn && lhs.end_sn == rhs.end_sn && lhs.part_index == rhs.part_index && lhs.parts == rhs.parts;
}


bool operator<(const SequenceRange & lhs, const SequenceRange & rhs)
{
    if (lhs.start_sn < rhs.start_sn)
    {
        return true;
    }
    else if (lhs.start_sn == rhs.start_sn)
    {
        return lhs.part_index < rhs.part_index;
    }
    else
    {
        return false;
    }
}

inline void SequenceRange::write(WriteBuffer & out) const
{
    DB::writeText(start_sn, out);
    DB::writeText(",", out);
    DB::writeText(end_sn, out);
    DB::writeText(",", out);
    DB::writeText(part_index, out);
    DB::writeText(",", out);
    DB::writeText(parts, out);
}

bool SequenceInfo::valid() const
{
    if (sequence_ranges.empty() && (!idempotent_keys || idempotent_keys->empty()))
    {
        return false;
    }

    for (const auto & seq_range: sequence_ranges)
    {
        if (!seq_range.valid())
        {
            return false;
        }
    }

    return true;
}

void SequenceInfo::write(WriteBuffer & out) const
{
    if (!valid())
    {
        return;
    }

    /// Format:
    /// version
    /// seqs:start_sn,end_sn,part_index,parts;
    /// keys:a,sn;b,sn;c,sn

    /// Version
    DB::writeText("1\n", out);

    DB::writeText("seqs:", out);
    /// Sequence ranges
    for (size_t index = 0, siz = sequence_ranges.size(); index < siz;)
    {
        sequence_ranges[index].write(out);
        if (++index < siz)
        {
            DB::writeText(";", out);
        }
    }

    if (!idempotent_keys)
    {
        out.finalize();
        return;
    }

    DB::writeText("\n", out);

    DB::writeText("keys:", out);
    /// Idempotent keys
    for (size_t index = 0, siz = idempotent_keys->size(); index < siz;)
    {
        DB::writeText(idempotent_keys->at(index).first, out);
        DB::writeText(",", out);
        DB::writeText(idempotent_keys->at(index).second, out);
        if (++index < siz)
        {
            DB::writeText(";", out);
        }
    }
    out.finalize();
}

std::shared_ptr<SequenceInfo> SequenceInfo::read(ReadBuffer & in)
{
    assertString("1\n", in);

    auto sequence_ranges = readSequenceRanges(in);

    std::shared_ptr<IdempotentKeys> idempotent_keys;
    if (!in.eof())
    {
        assertString("\n", in);
        idempotent_keys = readIdempotentKeys(in);
    }

    return std::make_shared<SequenceInfo>(std::move(sequence_ranges), idempotent_keys);
}

/// Data in parameter `sequences` will be reordered when merging
SequenceInfoPtr
mergeSequenceInfo(std::vector<SequenceInfoPtr> & sequences, Int64 committed_sn, UInt64 max_idempotent_keys, Poco::Logger * log)
{
    if (sequences.empty())
    {
        return nullptr;
    }

    /// Sort sequence according to sequence ID ranges
    std::sort(sequences.begin(), sequences.end(), [](const auto & lhs, const auto & rhs) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        if (lhs->sequence_ranges.empty())
        {
            return true;
        }

        if (rhs->sequence_ranges.empty())
        {
            return false;
        }

        return lhs->sequence_ranges[0].start_sn < rhs->sequence_ranges[0].start_sn;
    });

    auto sequence_ranges = mergeSequenceRanges(sequences, committed_sn, log);
    auto idempotent_keys = mergeIdempotentKeys(sequences, max_idempotent_keys, log);

    return std::make_shared<SequenceInfo>(std::move(sequence_ranges), idempotent_keys);
}

/// The `sequence_ranges` are supposed to have sequence numbers which are great than `committed`
std::pair<std::vector<SequenceRange>, Int64>
missingSequenceRanges(std::vector<SequenceRange> & sequence_ranges, Int64 committed, Poco::Logger * log)
{
    auto next_expecting_sn = committed + 1;
    SequenceRanges missing_ranges;
    SequenceRange prev_range;
    Int32 parts = 1;

    std::sort(sequence_ranges.begin(), sequence_ranges.end());

    for (size_t prev_index = 0, index = 0, siz = sequence_ranges.size(); index < siz; ++index)
    {
        const auto & cur_range = sequence_ranges[index];
        if (prev_range.start_sn == -1)
        {
            prev_range = cur_range;
        }
        else if (prev_range.start_sn == cur_range.start_sn)
        {
            assert(prev_range.end_sn == cur_range.end_sn);
            assert(prev_range.part_index < cur_range.part_index);
            ++parts;
        }
        else
        {
            /// Meet a new start_sn, calcuate if the parts in current sn
            /// are all committed
            if (prev_range.parts == parts)
            {
                assert (prev_range.start_sn >= next_expecting_sn);

                /// All parts in this sn range are committed
                if (prev_range.start_sn == next_expecting_sn)
                {
                    if (log)
                    {
                        LOG_INFO(
                            log,
                            "Parts in sn range=({}, {}) having total parts={} are all committed and there is no gap with previous "
                            "committed sn",
                            prev_range.start_sn,
                            prev_range.end_sn,
                            prev_range.parts);
                    }
                    next_expecting_sn = prev_range.end_sn + 1;
                }
                else
                {
                    if (log)
                    {
                        LOG_INFO(
                            log,
                            "Parts in sn range=({}, {}) having total parts={} are all committed but there are missing sn range=({}, {})",
                            prev_range.start_sn,
                            prev_range.end_sn,
                            prev_range.parts,
                            next_expecting_sn,
                            prev_range.start_sn - 1);
                    }
                    missing_ranges.emplace_back(next_expecting_sn, prev_range.start_sn - 1);
                }
            }
            else
            {
                /// Some parts in this sn range are missing
                /// Find the missing parts and add to `missing_ranges`
                Int32 next_expecting_part_index = 0;
                std::vector<String> missed_parts;

                for (size_t i = prev_index; i < index; ++i)
                {
                    if (sequence_ranges[i].part_index != next_expecting_part_index)
                    {
                        /// Collect the missing parts
                        for (auto j = next_expecting_part_index; j != sequence_ranges[i].part_index; ++j)
                        {
                            SequenceRange range_copy = prev_range;
                            range_copy.part_index = j;
                            missing_ranges.push_back(range_copy);
                            missed_parts.push_back(std::to_string(j));
                        }
                        next_expecting_part_index = sequence_ranges[i].part_index + 1;
                    }
                    else
                    {
                        ++next_expecting_part_index;
                    }
                }

                for (;next_expecting_part_index < sequence_ranges[prev_index].parts; ++next_expecting_part_index)
                {
                    SequenceRange range_copy = prev_range;
                    range_copy.part_index = next_expecting_part_index;
                    missing_ranges.push_back(range_copy);
                    missed_parts.push_back(std::to_string(next_expecting_part_index));
                }

                if (log)
                {
                    LOG_INFO(
                        log,
                        "Not all parts in sn range=({}, {}) are committed, total parts={}, missed parts=({})",
                        prev_range.start_sn,
                        prev_range.end_sn,
                        prev_range.parts,
                        boost::algorithm::join(missed_parts, ", "));
                }
            }

            /// Since here it starts a new start_sn, update prev_range, prev_index and parts
            prev_range = cur_range;
            prev_index = index;
            parts = 1;
        }
    }

    return {std::move(missing_ranges), next_expecting_sn};
}
}
