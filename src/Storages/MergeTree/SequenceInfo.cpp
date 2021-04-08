#include "SequenceInfo.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>
#include <common/logger_useful.h>

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
                seq_range.start_seq = parseIntStrict<Int64>(s, lpos, comma_pos);
                break;
            case 1:
                seq_range.end_seq = parseIntStrict<Int64>(s, lpos, comma_pos);
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

    /// Parse sequence
    if (data.empty())
    {
        return {};
    }

    SequenceRanges sequence_ranges;

    String::size_type siz = static_cast<String::size_type>(data.size());
    String::size_type last_pos = 0;

    while (last_pos < siz)
    {
        auto pos = data.find(';', last_pos);
        if (pos == String::npos)
        {
            sequence_ranges.push_back(parseSequenceRange(data, last_pos, siz));
            break;
        }
        else
        {
            sequence_ranges.push_back(parseSequenceRange(data, last_pos, pos));
            last_pos = pos + 1;
        }
    }

    return sequence_ranges;
}

std::shared_ptr<std::vector<String>> readIdempotentKeys(ReadBuffer & in)
{
    assertString("keys:", in);

    String keys;
    DB::readText(keys, in);

    if (keys.empty())
    {
        return {};
    }

    auto idempotent_keys = std::make_shared<std::vector<String>>();

    String::size_type siz = static_cast<String::size_type>(keys.size());
    String::size_type lpos = 0;

    for (; lpos < siz;)
    {
        auto comma_pos = keys.find(',', lpos);
        if (comma_pos == String::npos)
        {
            idempotent_keys->push_back(String(keys, lpos, siz - lpos));
        }
        else
        {
            idempotent_keys->push_back(String(keys, lpos, comma_pos - lpos));
            lpos = comma_pos + 1;
        }
    }

    return idempotent_keys;
}
}

inline void SequenceRange::write(WriteBuffer & out) const
{
    DB::writeText(start_seq, out);
    DB::writeText(",", out);
    DB::writeText(end_seq, out);
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
    /// seqs:start_seq,end_seq,part_index,parts;
    /// keys:a,b,c

    /// Version
    DB::writeText("1\n", out);

    DB::writeText("seqs:", out);
    /// Sequence ranges
    for (size_t index = 0, auto siz = ->size(); index < siz;)
    {
        [index].write(out);
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
    for (size_t index = 0, auto siz = idempotent_keys->size(); index < siz;)
    {
        DB::writeText(key, out);
        if (++index < siz)
        {
            DB::writeText(",", out);
        }
    }
    out.finalize();
}

std::shared_ptr<SequenceInfo> SequenceInfo::read(ReadBuffer & in)
{
    assertString("1\n", in);

    auto sequence_ranges = readSequenceRanges(in);

    std::shared_ptr<std::vector<String>> idempotent_keys;
    if (!in.eof())
    {
        assertString("\n", in);
        idempotent_keys = readIdempotentKeys(in);
    }

    return std::make_shared<SequenceInfo>(std::move(sequence_ranges), idempotent_keys);
}

SequenceRanges mergeSequenceRanges(const std::vector<SequenceInfoPtr> & sequences, Int64 last_committed_sn, Poco::Logger * log)
{
    SequenceRanges merged;

    SequenceRange last_seq_range;

    size_t total_ranges = 0;

    for (auto & seq_info : sequences)
    {
        total_ranges += seq_info->.size();

        /// Merge ranges
        for (const auto & next_seq_range : seq_info->)
        {
            assert(next_seq_ranges.valid());

            if (!last_seq_range.valid())
            {
                last_seq_range = next_seq_range;
            }
            else
            {
                /// There shall be no cases where there is sequence overlapping in a partition
                assert(last_seq_range.end_seq < next_seq_range.start_seq);
                last_seq_range = next_seq_range;
            }

            /// We don't check sequence gap here. There are 3 possible cases
            /// 1. The missing sequence is not committed yet (rarely)
            /// 2. It is casued by resending an / several idempotent blocks which will be deduped and ignored.
            /// 3. The Block whish has the missing sequence is distributed to a different partition
            /// Only for sequence ranges which are beyond the `last_committed_sn`, we need merge them and
            /// keep them around
            if (next_seq_range.end_seq > last_committed_sn)
            {
                /// We can still merge, because `last_committed_sn` confirms that
                /// all blocks with sequence IDs less than it are committed
                merged.push_back(next_seq_range);
            }
        }
    }

    if (log)
    {
        LOG_DEBUG(log, "Merge {} sequence ranges to {}", total_ranges, merged.size());
    }

    return merged;
}

std::shared_ptr<std::vector<String>>
mergeIdempotentKeys(std::vector<SequenceInfoPtr> & sequences, Int64 max_idempotent_keys, Poco::Logger * log)
{
    auto idempotent_keys = std::make_shared<std::vector<String>>();

    std::vector<SequenceInfoPtr>::iterator start_iter = sequences.rend();

    size_t start_pos = 0;
    size_t key_count = 0;

    for (auto iter = sequences.rbegin(); iter != sequences.rend(); ++iter)
    {
        if (iter->idempotent_keys)
        {
            key_count += iter->idempotent_keys->count();

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

    for (; start_iter != sequences.rbegin(); --start_iter)
    {
        if (start_pos > 0)
        {
            /// We will need fist skipping `start_pos` elements
            size_t count = 0;
            for (auto & key : *(*start_iter)->idempotent_keys)
            {
                if (count++ < start_pos)
                {
                    continue;
                }
                idempotent_keys->push_back(std::move(key));
                assert(key.empty());
            }

            start_pos = 0;
        }
        else if ((*start_iter)->idempotent_keys)
        {
            for (auto & key : (*start_iter)->idempotent_keys)
            {
                idempotent_keys->push_back(std::move(key));
                assert(key.empty());
            }
        }
    }

    if ((*sequences.rbegin())->idempotent_keys)
    {
        for (auto & key : (*sequences.rbegin())->idempotent_keys)
        {
            idempotent_keys->push_back(std::move(key));
            assert(key.empty());
        }
    }

    if (log)
    {
        LOG_DEBUG(log, "Merge {} idempotent keys to {}", key_count, idempotent_keys->size());
    }

    return idempotent_keys;
}

SequenceInfoPtr
mergeSequenceInfo(std::vector<SequenceInfoPtr> & sequences, Int64 last_commit_sn, Int64 max_idempotent_keys, Poco::Logger * log)
{
    /// Sort sequence according to sequence ID ranges
    std::sort(sequences.begin(), sequences.end(), [](const auto & lhs, const auto & rhs) {
        return lhs->sequence_ranges[0].start_seq < rhs->sequence_ranges[0].start_seq;
    });

    auto sequence_ranges = mergeSequenceRanges(sequences, last_commit_sn, log);
    auto idempotent_keys = mergeIdempotentKeys(sequences, max_idempotent_keys, log);

    return std::make_shared<SequenceInfo>(std::move(sequence_ranges), idempotent_keys);
}
}
