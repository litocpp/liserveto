module;
#ifdef LITO_FEAT_JSON
export module liserveto:json;
export import :endpoint;
export import rstd.json;
using namespace rstd::prelude;
using namespace rstd::literals;

export namespace liserveto
{
struct JsonBody {
    rstd::json::Value value;
    lihttpto::Headers trailers;
};
auto read_json(Request& request, u64 limit, rstd::json::ParseOptions options = {})
    -> rstd::async::coro<Result<JsonBody, Error>> {
    auto collected = co_await request.collect(limit);
    if (collected.is_err()) co_return Err(Error { ErrorKind::Body, Some(collected.unwrap_err()) });
    auto text = rstd::str_::from_utf8(collected->data.as_slice());
    if (text.is_err()) co_return Err(Error { ErrorKind::InvalidRequest });
    auto parsed = rstd::json::from_str(*text, options);
    if (parsed.is_err()) co_return Err(Error { ErrorKind::InvalidRequest });
    co_return Ok(JsonBody { rstd::move(parsed).unwrap(), rstd::move(collected->trailers) });
}
auto json_response(const rstd::json::Value& value) -> Response {
    auto text     = rstd::json::to_string(value);
    auto response = Response::bytes(rstd::bytes::Bytes::copy_from_slice(text.as_str().as_bytes()));
    response.head.headers.add("Content-Type"_str, "application/json"_str).unwrap();
    return response;
}
} // namespace liserveto
#endif
