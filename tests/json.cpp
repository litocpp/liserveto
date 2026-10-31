#ifdef LITO_FEAT_JSON
#include <rstd/test/gtest.hpp>
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace liserveto;

TEST(JsonEndpoint, ActualByteBudgetAndParseFailureAreDistinct) {
    auto make = [](ref<str> body) {
        lihttpto::RequestHeadDecoder decoder;
        decoder.feed("POST / HTTP/1.1\r\nHost: local\r\n\r\n"_str.as_bytes()).unwrap();
        return Request(decoder.take().unwrap(),
                       IncomingBody::from_body(
                           Body::from_bytes(rstd::bytes::Bytes::copy_from_slice(body.as_bytes()))),
                       Shutdown::make().unwrap());
    };
    auto runtime  = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto request  = make("{\"value\":42}"_str);
    auto value    = runtime.block_on(read_json(request, u64(32))).unwrap();
    auto response = json_response(value.value);
    EXPECT_EQ(*response.head.headers.get_unique_text("content-type"_str).unwrap(),
              "application/json"_str);
    EXPECT_EQ(runtime.block_on(response.body.collect(u64(32))).unwrap().data.as_slice(),
              "{\"value\":42}"_str.as_bytes());
    auto limited = make("{\"value\":42}"_str);
    EXPECT_EQ(*runtime.block_on(read_json(limited, u64(2))).unwrap_err().body,
              BodyError::LimitExceeded);
    auto invalid = make("{"_str);
    EXPECT_EQ(runtime.block_on(read_json(invalid, u64(32))).unwrap_err().kind,
              ErrorKind::InvalidRequest);
    EXPECT_EQ(runtime.block_on(read_json(request, u64(32))).unwrap_err().kind, ErrorKind::Body);
}
TEST(JsonEndpoint, ParsedValueRetainsTrailerMetadata) {
    lihttpto::RequestHeadDecoder decoder;
    decoder.feed("POST / HTTP/1.1\r\nHost: local\r\n\r\n"_str.as_bytes()).unwrap();
    lihttpto::Headers trailers;
    trailers.add("X-End"_str, "yes"_str).unwrap();
    Vec<BodyItem> parts;
    parts.push(
        Ok(lihttpto::BodyFrame::Data(rstd::bytes::Bytes::copy_from_slice("{}"_str.as_bytes()))));
    parts.push(Ok(lihttpto::BodyFrame::Trailers(rstd::move(trailers))));
    auto incoming =
        IncomingBody::from_body(Body::from_stream(rstd::stream::iter(rstd::move(parts))));
    Request request(decoder.take().unwrap(), rstd::move(incoming), Shutdown::make().unwrap());
    auto    runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto    parsed  = runtime.block_on(read_json(request, u64(2))).unwrap();
    EXPECT_EQ(*parsed.trailers.get_unique_text("X-End"_str).unwrap(), "yes"_str);
}
TEST(JsonEndpoint, CallerParseOptionsRejectDuplicateKeysAndIntegerOverflow) {
    auto make = [](ref<str> body) {
        lihttpto::RequestHeadDecoder decoder;
        decoder.feed("POST / HTTP/1.1\r\nHost: local\r\n\r\n"_str.as_bytes()).unwrap();
        return Request(decoder.take().unwrap(),
                       IncomingBody::from_body(
                           Body::from_bytes(rstd::bytes::Bytes::copy_from_slice(body.as_bytes()))),
                       Shutdown::make().unwrap());
    };
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    rstd::json::ParseOptions strict;
    strict.reject_duplicate_keys   = true;
    strict.reject_integer_overflow = true;
    auto duplicate                 = make(R"({"value":1,"value":2})"_str);
    EXPECT_EQ(runtime.block_on(read_json(duplicate, u64(128), strict)).unwrap_err().kind,
              ErrorKind::InvalidRequest);
    auto overflow = make(R"({"value":18446744073709551616})"_str);
    EXPECT_EQ(runtime.block_on(read_json(overflow, u64(128), strict)).unwrap_err().kind,
              ErrorKind::InvalidRequest);
    auto valid = make(R"({"value":18446744073709551615})"_str);
    EXPECT_EQ(runtime.block_on(read_json(valid, u64(128), strict))
                  .unwrap()
                  .value.get("value"_str)
                  .unwrap()
                  ->as_u64()
                  .unwrap(),
              u64::MAX);
    auto compatible = make(R"({"value":1,"value":2})"_str);
    EXPECT_TRUE(runtime.block_on(read_json(compatible, u64(128))).is_ok());
}
#endif
