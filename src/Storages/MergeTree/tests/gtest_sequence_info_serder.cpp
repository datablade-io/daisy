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

TEST(SequenceInfoMerge, Merge)
{
   /// seqs:1,1,0,1
   /// seqs:2,2,0,1
   /// committed_sn : 0
   /// => seqs:1,1,0,1;2,2,0,1
   DB::SequenceRange range1{1, 1, 0, 1};
   DB::SequenceRange range2{2, 2, 0, 1};

   auto seq_info1 = std::shared_ptr<DB::SequenceInfo>(new DB::SequenceInfo({range1}, {}));
   auto seq_info2 = std::shared_ptr<DB::SequenceInfo>(new DB::SequenceInfo({range2}, {}));
   std::vector<DB::SequenceInfoPtr> sequences = {seq_info2, seq_info1};

   auto merged = DB::mergeSequenceInfo(sequences, 0, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_EQ(merged->sequence_ranges.size(), 2);
   EXPECT_EQ(merged->sequence_ranges[0], range1);
   EXPECT_EQ(merged->sequence_ranges[1], range2);
   EXPECT_TRUE(!merged->idempotent_keys);

   /// seqs:1,1,0,1
   /// seqs:2,2,0,1
   /// committed_sn : 1
   /// => seqs:2,2,0,1
   merged = DB::mergeSequenceInfo(sequences, 1, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_EQ(merged->sequence_ranges.size(), 1);
   EXPECT_EQ(merged->sequence_ranges[0], range2);
   EXPECT_TRUE(!merged->idempotent_keys);

   /// seqs:1,1,0,1
   /// seqs:2,2,0,1
   /// committed_sn : 2
   /// => seqs:
   merged = DB::mergeSequenceInfo(sequences, 2, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(!merged->idempotent_keys);

   /// seqs:1,1,0,1
   /// seqs:2,2,0,1
   /// committed_sn : 3
   /// => seqs:
   merged = DB::mergeSequenceInfo(sequences, 3, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(!merged->idempotent_keys);

   auto prepare_keys = []() -> std::pair<std::shared_ptr<std::vector<String>>, std::shared_ptr<std::vector<String>>>
   {
       auto keys1 = std::make_shared<std::vector<String>>();
       keys1->push_back("idem1");
       auto keys2 = std::make_shared<std::vector<String>>();
       keys2->push_back("idem2");
       return {keys1, keys2};
   };

   /// seqs:1,1,0,1\nkeys:idem1
   /// seqs:2,2,0,1\nkeys:idem2
   /// committed_sn : 3
   /// max_idempotent_keys: 0
   /// => seqs:
   ///    keys:
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 0, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_TRUE(merged->idempotent_keys->empty());

   /// seqs:1,1,0,1\nkeys:idem1
   /// seqs:2,2,0,1\nkeys:idem2
   /// committed_sn : 3
   /// max_idempotent_keys: 1
   /// => seqs:
   ///    keys:idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 1, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 1);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem2");

   /// seqs:1,1,0,1\nkeys:idem1
   /// seqs:2,2,0,1\nkeys:idem2
   /// committed_sn : 3
   /// max_idempotent_keys: 2
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 2, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 2);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem1");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem2");

   /// seqs:1,1,0,1\nkeys:idem1
   /// seqs:2,2,0,1\nkeys:idem2
   /// committed_sn : 3
   /// max_idempotent_keys: 3
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 2);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem1");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem2");

   /// seqs:2,2,0,1\nkeys:idem2
   /// seqs:1,1,0,1\nkeys:idem1
   /// committed_sn : 3
   /// max_idempotent_keys: 1
   /// => seqs:
   ///    keys:idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   sequences[0].swap(sequences[1]);
   merged = DB::mergeSequenceInfo(sequences, 3, 1, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 1);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem2");

   /// seqs:2,2,0,1\nkeys:idem2
   /// seqs:1,1,0,1\nkeys:idem1
   /// committed_sn : 3
   /// max_idempotent_keys: 2
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   sequences[0].swap(sequences[1]);
   merged = DB::mergeSequenceInfo(sequences, 3, 2, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 2);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem1");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem2");

   /// seqs:2,2,0,1\nkeys:idem2
   /// seqs:1,1,0,1\nkeys:idem1
   /// committed_sn : 3
   /// max_idempotent_keys: 3
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       seq_info2->idempotent_keys = keys2;
   }

   sequences[0].swap(sequences[1]);
   merged = DB::mergeSequenceInfo(sequences, 3, 3, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 2);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem1");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem2");

   /// seqs:1,1,0,1\nkeys:idem1,idem2,idem3,idem4,idem5,idem6
   /// seqs:2,2,0,1\nkeys:idem22
   /// committed_sn : 3
   /// max_idempotent_keys: 5
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       for (Int32 i = 2; i < 7; ++i)
       {
           keys1->push_back("idem" + std::to_string(i));
       }
       keys2->at(0) = "idem22";
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 5, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 5);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem3");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem4");
   EXPECT_EQ(merged->idempotent_keys->at(2), "idem5");
   EXPECT_EQ(merged->idempotent_keys->at(3), "idem6");
   EXPECT_EQ(merged->idempotent_keys->at(4), "idem22");

   /// seqs:1,1,0,1\nkeys:idem1,idem2,idem3,idem4,idem5,idem6
   /// seqs:2,2,0,1\nkeys:idem22
   /// committed_sn : 3
   /// max_idempotent_keys: 7
   /// => seqs:
   ///    keys:idem1,idem2
   {
       auto [keys1, keys2] = prepare_keys();
       seq_info1->idempotent_keys = keys1;
       for (Int32 i = 2; i < 7; ++i)
       {
           keys1->push_back("idem" + std::to_string(i));
       }
       keys2->at(0) = "idem22";
       seq_info2->idempotent_keys = keys2;
   }

   merged = DB::mergeSequenceInfo(sequences, 3, 7, nullptr);
   EXPECT_TRUE(merged);
   EXPECT_TRUE(merged->sequence_ranges.empty());
   EXPECT_TRUE(merged->idempotent_keys);
   EXPECT_EQ(merged->idempotent_keys->size(), 7);
   EXPECT_EQ(merged->idempotent_keys->at(0), "idem1");
   EXPECT_EQ(merged->idempotent_keys->at(1), "idem2");
   EXPECT_EQ(merged->idempotent_keys->at(2), "idem3");
   EXPECT_EQ(merged->idempotent_keys->at(3), "idem4");
   EXPECT_EQ(merged->idempotent_keys->at(4), "idem5");
   EXPECT_EQ(merged->idempotent_keys->at(5), "idem6");
   EXPECT_EQ(merged->idempotent_keys->at(6), "idem22");
}
