#include "SequenceInfo.h"

#include <Common/parseIntStrict.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
std::shared_ptr<std::vector<std::pair<Int64, Int64>>> readSequenceRanges(ReadBuffer & in)
{
    String sequences;
    DB::readText(sequences, in);

    /// Parse sequence
    if (sequences.empty())
    {
        throw Exception("Invalid sequences", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    auto parse_range = [](const String & s, String::size_type lpos, String::size_type rpos) -> std::pair<Int64, Int64> {
        auto dash_pos = s.find('-', lpos);
        if (dash_pos == String::npos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        if (dash_pos > rpos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        auto start = parseIntStrict<Int64>(s, lpos, dash_pos);
        auto end = parseIntStrict<Int64>(s, dash_pos + 1, rpos);
        return {start, end};
    };

    auto sequence_ranges = std::make_shared<std::vector<std::pair<Int64, Int64>>>();

    String::size_type siz = static_cast<String::size_type>(sequences.size());
    String::size_type last_pos = 0;

    while (last_pos < siz)
    {
        auto pos = sequences.find(',', last_pos);
        if (pos == String::npos)
        {
            sequence_ranges->push_back(parse_range(sequences, last_pos, sequences.size()));
            break;
        }
        else
        {
            sequence_ranges->push_back(parse_range(sequences, last_pos, pos));
            last_pos = pos + 1;
        }
    }

    return sequence_ranges;
}

std::pair<Int32, Int32> readPartInfo(ReadBuffer & in)
{
    String part;
    DB::readText(part, in);

    if (part.empty())
    {
        throw Exception("Empty part", ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    Int32 part_index = 0;

    auto pos = part.find(',');
    if (pos == String::npos)
    {
        part_index = parseIntStrict<Int32>(part, 0, pos);
    }

    Int32 parts = parseIntStrict<Int32>(part, pos + 1, part.size());

    return {part_index, parts};
}

std::shared_ptr<std::vector<String>> readIdempotentKeys(ReadBuffer & in)
{
    String keys;
    DB::readText(keys, in);

    if (keys.empty())
    {
        return {};
    }

    auto idempotent_keys = std::make_shared<std::vector<String>>();
    boost::algorithm::split(*idempotent_keys, keys, boost::is_any_of(","));

    return idempotent_keys;
}
}

bool SequenceInfo::valid() const
{
    return sequence_ranges && parts > 0 && part_index >= 0 && part_index < parts;
}

void SequenceInfo::write(WriteBuffer & out) const
{
    if (!valid())
    {
        return;
    }

    /// Format:
    /// version
    /// sequence ranges
    /// part_index,parts
    /// idempotent_keys

    /// Version
    DB::writeText("1\n", out);

    /// Sequence ranges
    size_t index = 0;
    size_t siz = sequence_ranges->size();
    for (const auto & seq_range : *sequence_ranges)
    {
        DB::writeText(seq_range.first, out);
        DB::writeText("-", out);
        DB::writeText(seq_range.second, out);

        if (++index < siz)
        {
            DB::writeText(",", out);
        }
    }
    DB::writeText("\n", out);

    /// Part
    DB::writeText(part_index, out);
    DB::writeText(",", out);
    DB::writeText(parts, out);

    if (!idempotent_keys)
    {
        out.finalize();
        return;
    }

    DB::writeText("\n", out);

    /// Idempotent keys
    index = 0;
    siz = idempotent_keys->size();
    for (const auto & key : *idempotent_keys)
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
    assertString("\n", in);

    auto [part_index, parts] = readPartInfo(in);

    std::shared_ptr<std::vector<String>> idempotent_keys;
    if (!in.eof())
    {
        assertString("\n", in);
        idempotent_keys = readIdempotentKeys(in);
    }

    auto si = std::make_shared<SequenceInfo>(sequence_ranges, idempotent_keys);
    si->part_index = part_index;
    si->parts = parts;

    return si;
}
}
