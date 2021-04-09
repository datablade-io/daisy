#include <Storages/MergeTree/SequenceInfo.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>


TEST(SequenceInfoSerializationDeserialization, Serder)
{
    DB::SequenceRange seq_range{1, 3, 0, 3};
    DB::SequenceRange seq_range2{4, 5, 1, 2};

    DB::SequenceInfo si({seq_range}, {});

    /// 1 sequence range + no idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out1;
    si.write(out1);

    DB::String si_str = "1\nseqs:1,3,0,3";
    EXPECT_EQ(out1.str(), si_str);

    // Der
    DB::ReadBufferFromString in1{si_str};
    auto sip = DB::SequenceInfo::read(in1);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->sequence_ranges.size(), 1);
    EXPECT_EQ(sip->sequence_ranges[0], seq_range);
    EXPECT_TRUE(!sip->idempotent_keys);

    /// 2 sequence ranges + no idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out2;
    si.sequence_ranges.push_back(seq_range2);
    si.write(out2);

    DB::String si_str2 = "1\nseqs:1,3,0,3;4,5,1,2";
    EXPECT_EQ(out2.str(), si_str2);

    // Der
    DB::ReadBufferFromString in2{si_str2};
    sip = DB::SequenceInfo::read(in2);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->sequence_ranges.size(), 2);
    EXPECT_EQ(sip->sequence_ranges[0], seq_range);
    EXPECT_EQ(sip->sequence_ranges[1], seq_range2);

    /// 2 sequence ranges + 1 idempotent key
    /// Ser
    DB::WriteBufferFromOwnString out3;
    si.idempotent_keys = std::make_shared<std::vector<DB::String>>();
    si.idempotent_keys->push_back("idem1");
    si.write(out3);

    DB::String si_str3 = "1\nseqs:1,3,0,3;4,5,1,2\nkeys:idem1";
    EXPECT_EQ(out3.str(), si_str3);

    // Der
    DB::ReadBufferFromString in3{si_str3};
    sip = DB::SequenceInfo::read(in3);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->sequence_ranges.size(), 2);
    EXPECT_EQ(sip->sequence_ranges[0], seq_range);
    EXPECT_EQ(sip->sequence_ranges[1], seq_range2);
    EXPECT_TRUE(sip->idempotent_keys);
    EXPECT_EQ(sip->idempotent_keys->size(), 1);
    EXPECT_EQ(sip->idempotent_keys->at(0), "idem1");

    /// 2 sequence ranges + 2 idempotent keys
    /// Ser
    DB::WriteBufferFromOwnString out4;
    si.idempotent_keys->push_back("idem2");
    si.write(out4);

    DB::String si_str4 = "1\nseqs:1,3,0,3;4,5,1,2\nkeys:idem1,idem2";
    EXPECT_EQ(out4.str(), si_str4);

    /// Der
    DB::ReadBufferFromString in4{si_str4};
    sip = DB::SequenceInfo::read(in4);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->sequence_ranges.size(), 2);
    EXPECT_EQ(sip->sequence_ranges[0], seq_range);
    EXPECT_EQ(sip->sequence_ranges[1], seq_range2);
    EXPECT_TRUE(sip->idempotent_keys);
    EXPECT_EQ(sip->idempotent_keys->size(), 2);
    EXPECT_EQ(sip->idempotent_keys->at(0), "idem1");
    EXPECT_EQ(sip->idempotent_keys->at(1), "idem2");

    /// 0 sequence ranges + 2 idempotent keys
    /// Ser
    DB::WriteBufferFromOwnString out5;
    si.sequence_ranges.clear();
    si.write(out5);

    DB::String si_str5 = "1\nseqs:\nkeys:idem1,idem2";
    EXPECT_EQ(out5.str(), si_str5);

    /// Der
    DB::ReadBufferFromString in5{si_str5};
    sip = DB::SequenceInfo::read(in5);
    EXPECT_TRUE(sip->valid());
    EXPECT_EQ(sip->sequence_ranges.size(), 0);
    EXPECT_TRUE(sip->idempotent_keys);
    EXPECT_EQ(sip->idempotent_keys->size(), 2);
    EXPECT_EQ(sip->idempotent_keys->at(0), "idem1");
    EXPECT_EQ(sip->idempotent_keys->at(1), "idem2");

    /// Non-happy path
    DB::SequenceInfo si2({}, nullptr);
    EXPECT_TRUE(!si2.valid());

    DB::WriteBufferFromOwnString out11;
    si2.write(out11);
    EXPECT_EQ(out11.str(), "");
}
