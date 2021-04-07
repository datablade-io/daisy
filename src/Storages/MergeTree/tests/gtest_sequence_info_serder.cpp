#include <Storages/MergeTree/SequenceInfo.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>


TEST(SequenceInfoSerializationDeserialization, Serder)
{
    std::pair<Int64, Int64> seq_range{1,3};
    std::pair<Int64, Int64> seq_range2{4, 5};

    DB::SequenceInfo si(seq_range, {});
    si.part_index = 0;
    si.parts = 3;

    /// One sequence range + no idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out1;
    si.write(out1);

    DB::String si_str = "1\n1-3\n0,3";
    EXPECT_EQ(out1.str(), si_str);

    // Der
    DB::ReadBufferFromString in1{si_str};
    auto sip = DB::SequenceInfo::read(in1);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->part_index, si.part_index);
    EXPECT_EQ(sip->parts, si.parts);
    EXPECT_EQ(sip->sequence_ranges->size(), 1);
    EXPECT_EQ(sip->sequence_ranges->at(0), seq_range);
    EXPECT_TRUE(!sip->idempotent_keys);

    /// Two sequence ranges + no idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out2;
    si.sequence_ranges->emplace_back(4, 5);
    si.write(out2);

    DB::String si_str2 = "1\n1-3,4-5\n0,3";
    EXPECT_EQ(out2.str(), si_str2);

    // Der
    DB::ReadBufferFromString in2{si_str2};
    sip = DB::SequenceInfo::read(in2);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->part_index, si.part_index);
    EXPECT_EQ(sip->parts, si.parts);
    EXPECT_EQ(sip->sequence_ranges->size(), 2);
    EXPECT_EQ(sip->sequence_ranges->at(0), seq_range);
    EXPECT_EQ(sip->sequence_ranges->at(1), seq_range2);

    /// Two sequence ranges + 1 idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out3;
    si.idempotent_keys = std::make_shared<std::vector<DB::String>>();
    si.idempotent_keys->push_back("idem1");
    si.write(out3);

    DB::String si_str3 = "1\n1-3,4-5\n0,3\nidem1";
    EXPECT_EQ(out3.str(), si_str3);

    // Der
    DB::ReadBufferFromString in3{si_str3};
    sip = DB::SequenceInfo::read(in3);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->part_index, si.part_index);
    EXPECT_EQ(sip->parts, si.parts);
    EXPECT_EQ(sip->sequence_ranges->size(), 2);
    EXPECT_EQ(sip->sequence_ranges->at(0), seq_range);
    EXPECT_EQ(sip->sequence_ranges->at(1), seq_range2);
    EXPECT_TRUE(sip->idempotent_keys);
    EXPECT_EQ(sip->idempotent_keys->size(), 1);
    EXPECT_EQ(sip->idempotent_keys->at(0), "idem1");

    /// Two sequence ranges + 2 idempotent keys
    /// Ser
    DB::WriteBufferFromOwnString out4;
    si.idempotent_keys->push_back("idem2");
    si.write(out4);

    DB::String si_str4 = "1\n1-3,4-5\n0,3\nidem1,idem2";
    EXPECT_EQ(out4.str(), si_str4);

    /// Der
    DB::ReadBufferFromString in4{si_str4};
    sip = DB::SequenceInfo::read(in4);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->part_index, si.part_index);
    EXPECT_EQ(sip->parts, si.parts);
    EXPECT_EQ(sip->sequence_ranges->size(), 2);
    EXPECT_EQ(sip->sequence_ranges->at(0), seq_range);
    EXPECT_EQ(sip->sequence_ranges->at(1), seq_range2);
    EXPECT_TRUE(sip->idempotent_keys);
    EXPECT_EQ(sip->idempotent_keys->size(), 2);
    EXPECT_EQ(sip->idempotent_keys->at(0), "idem1");
    EXPECT_EQ(sip->idempotent_keys->at(1), "idem2");

    /// Non-happy path
    DB::SequenceInfo si2(nullptr, nullptr);
    EXPECT_TRUE(!si2.valid());

    DB::WriteBufferFromOwnString out11;
    si2.write(out11);
    EXPECT_EQ(out11.str(), "");

    si2.sequence_ranges = si.sequence_ranges;
    EXPECT_TRUE(!si2.valid());
}
