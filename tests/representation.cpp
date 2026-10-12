#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;
using namespace lihttpto;

auto source_metadata() -> RepresentationMetadata {
    RepresentationMetadata metadata;
    metadata.length        = u64(10);
    metadata.etag          = Some(EntityTag::parse("\"content\""_bytes).unwrap());
    metadata.last_modified = http_date("Sun, 06 Nov 1994 08:49:37 GMT"_str);
    return metadata;
}
auto selected_body(ByteRange range) -> Body {
    auto bytes    = "0123456789"_bytes;
    auto selected = slice<u8>::from_raw_parts(bytes.as_raw_ptr() + range.offset.to_primitive(),
                                              usize(range.length.to_primitive()));
    return Body::from_bytes(rstd::bytes::Bytes::copy_from_slice(selected));
}
auto encoded_metadata(const liserveto::Response& response, ref<str> method) -> String {
    auto encoder =
        ResponseEncoder::make(
            response.head, Method::parse(method).unwrap(), Version::Http11, response.content_length)
            .unwrap();
    auto bytes = encoder.take_head().unwrap();
    return String::make(rstd::str_::from_utf8(bytes.as_slice()).unwrap());
}
TEST(RepresentationResponse, ReusesOwnedConditionsAndStreamsOnlySelectedBytes) {
    Option<RequestConditions> conditions;
    {
        Headers request;
        request.add("range"_str, "bytes=2-4"_str).unwrap();
        request.add("if-range"_str, "\"content\""_str).unwrap();
        conditions = Some(RequestConditions::parse(request).unwrap());
    }
    int  calls {};
    auto response =
        representation_response("GET"_str, *conditions, source_metadata(), [&](ByteRange range) {
            ++calls;
            EXPECT_EQ(range.offset, u64(2));
            EXPECT_EQ(range.length, u64(3));
            return selected_body(range);
        });
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(response.head.status.value(), u16(206));
    auto head = encoded_metadata(response, "GET"_str);
    EXPECT_TRUE(head->contains("content-range: bytes 2-4/10\r\n"_str));
    EXPECT_TRUE(head->contains("Content-Length: 3\r\n"_str));
    EXPECT_TRUE(head->contains("etag: \"content\"\r\n"_str));
    EXPECT_TRUE(head->contains("last-modified: Sun, 06 Nov 1994 08:49:37 GMT\r\n"_str));
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto body    = runtime.block_on(response.body.collect(u64(3))).unwrap();
    EXPECT_TRUE(body.data.as_slice() == "234"_bytes);
}
TEST(RepresentationResponse, HeadAndTerminalStatusesNeverCreateContentSource) {
    struct Case {
        ref<str> method, name, value;
        u16      status;
    };
    for (auto item :
         array<Case, 4> { Case { "HEAD"_str, "range"_str, "bytes=2-4"_str, u16(200) },
                          Case { "GET"_str, "if-none-match"_str, "W/\"content\""_str, u16(304) },
                          Case { "GET"_str, "if-match"_str, "\"other\""_str, u16(412) },
                          Case { "GET"_str, "range"_str, "bytes=10-"_str, u16(416) } }) {
        Headers request, extra;
        request.add(item.name, item.value).unwrap();
        extra.add("content-type"_str, "text/plain"_str).unwrap();
        auto conditions = RequestConditions::parse(request).unwrap();
        int  calls {};
        auto response = representation_response(
            item.method,
            conditions,
            source_metadata(),
            [&](ByteRange range) {
                ++calls;
                return selected_body(range);
            },
            rstd::move(extra));
        EXPECT_EQ(calls, 0);
        EXPECT_TRUE(response.body.is_empty());
        EXPECT_EQ(response.head.status.value(), item.status);
        auto head = encoded_metadata(response, item.method);
        if (item.status == u16(200)) EXPECT_TRUE(head->contains("Content-Length: 10\r\n"_str));
        if (item.status == u16(304)) {
            EXPECT_FALSE(head->contains("Content-Length:"_str));
            EXPECT_FALSE(head->contains("content-type:"_str));
        }
        if (item.status == u16(416))
            EXPECT_TRUE(head->contains("content-range: bytes */10\r\n"_str));
    }
}
TEST(RepresentationResponse, MissingMethodAndEmptyRepresentationChecksPrecedeSourceCreation) {
    Headers headers;
    headers.add("if-none-match"_str, "*"_str).unwrap();
    auto conditions = RequestConditions::parse(headers).unwrap();
    auto metadata   = source_metadata();
    int  calls {};
    auto source = [&](ByteRange range) {
        ++calls;
        return selected_body(range);
    };
    metadata.exists = false;
    auto missing    = representation_response("GET"_str, conditions, metadata, source);
    EXPECT_EQ(missing.head.status.value(), u16(404));
    metadata.exists = true;
    auto rejected   = representation_response("POST"_str, conditions, metadata, source);
    EXPECT_EQ(rejected.head.status.value(), u16(405));
    EXPECT_EQ(*header_text(rejected.head.headers, "allow"_str), "GET, HEAD"_str);
    metadata.length = u64();
    auto empty      = representation_response(
        "GET"_str, RequestConditions::parse(Headers {}).unwrap(), metadata, source);
    EXPECT_TRUE(empty.body.is_empty());
    EXPECT_EQ(*empty.content_length, u64());
    EXPECT_EQ(calls, 0);
}
