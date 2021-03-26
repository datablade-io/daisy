#include <gtest/gtest.h>

#include <IO/JSON2QueryReadBuffer.h>

#include <Common/typeid_cast.h>
#include <Common/PODArray.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>

#include <Poco/File.h>
#include <stdexcept>

using namespace DB;

TEST(JSON2QueryReadBuffer, ReadTest)
{
    String req = "{\n"
                 "    \"columns\": [\"a\"],\n"
                 "    \"data\": [[20], [21]]\n"
                 "}";
    std::unique_ptr<ReadBuffer> in = std::make_unique<ReadBufferFromString>(req);
    std::unique_ptr<JSON2QueryReadBuffer> buf = std::make_unique<JSON2QueryReadBuffer>(
        std::move(in), "test");
    if(!buf->hasPendingData())
        buf->next();
    StringRef prefix = StringRef(buf->position(), buf->buffer().end() - buf->position());
    ASSERT_EQ(prefix.toString(), "INSERT INTO test (\"a\") FORMAT JSONCompactEachRow ");
    buf->position() = buf->buffer().end();
    buf->next();
    StringRef cols = StringRef(buf->position(), buf->buffer().end() - buf->position());
    ASSERT_EQ(cols.toString(), "[[20], [21]]\n}");
}

TEST(JSON2QueryReadBuffer, WorkWithOtherBuffer)
{
    String req = "{\n"
                 "    \"columns\": [\"a\"],\n"
                 "    \"data\": [[20], [21]]\n"
                 "}";
    std::unique_ptr<ReadBuffer> in = std::make_unique<ReadBufferFromString>(req);
    std::unique_ptr<JSON2QueryReadBuffer> buf = std::make_unique<JSON2QueryReadBuffer>(
        std::move(in), "test");
    if(!buf->hasPendingData())
        buf->next();
    std::cout << "available: " << buf->available() << ", pos: " << buf->position() << ", end: " << buf->buffer().end() << std::endl;
    PODArray<char> parse_buf;
    WriteBufferFromVector<PODArray<char>> out(parse_buf);
    LimitReadBuffer limit(*buf, 100, false);
    copyData(limit, out);
    out.finalize();
    auto parse = std::string(parse_buf.data(), parse_buf.size());
    ASSERT_EQ(parse, "INSERT INTO test (\"a\") FORMAT JSONCompactEachRow [[20], [21]]\n}");
}


TEST(JSON2QueryReadBuffer, ConcatedBuffer)
{
    String req1 = "{\n"
                  "    \"columns\": [\"a";
    String req2 = "\"],\n"
                  "    \"data\": [[20";
    String req3 = "], [21]]\n"
                  "}";
    std::unique_ptr<ReadBuffer> in1 = std::make_unique<ReadBufferFromString>(req1);
    std::unique_ptr<ReadBuffer> in2 = std::make_unique<ReadBufferFromString>(req2);
    std::unique_ptr<ReadBuffer> in3 = std::make_unique<ReadBufferFromString>(req3);
    std::vector<ReadBuffer *> buffers {&*in1, &*in2, &*in3};
    std::unique_ptr<ConcatReadBuffer> in = std::make_unique<ConcatReadBuffer>(buffers);

    std::unique_ptr<JSON2QueryReadBuffer> buf = std::make_unique<JSON2QueryReadBuffer>(
        std::move(in), "test");
    if(!buf->hasPendingData())
        buf->next();
    std::cout << "available: " << buf->available() << ", pos: " << buf->position() << ", end: " << buf->buffer().end() << std::endl;
    PODArray<char> parse_buf;
    WriteBufferFromVector<PODArray<char>> out(parse_buf);
    // 31: (; 32: 2, 33:20, 34: ], 35: , 36: \s, 37:[, 40 ], 41], 42:\n, 43: ]
    LimitReadBuffer limit(*buf, 100, false);
    copyData(limit, out);
    out.finalize();
    auto parse = std::string(parse_buf.data(), parse_buf.size());
    ASSERT_EQ(parse, "INSERT INTO test (\"a\") FORMAT JSONCompactEachRow [[20], [21]]\n}");
}
