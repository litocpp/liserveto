export module liserveto:static_files.representation;
export import :response;
using namespace rstd::prelude;
using namespace rstd::literals;

export namespace liserveto
{
// The factory returns a lazy body for the selected range, not an already running read.
template<class F>
    requires requires(F factory, lihttpto::ByteRange range) {
        { factory(range) } -> rstd::mtp::same_as<Body>;
    }
auto representation_response(ref<str>                                method,
                             const lihttpto::RequestConditions&      conditions,
                             const lihttpto::RepresentationMetadata& metadata,
                             F                                       body_factory,
                             lihttpto::Headers                       headers = {}) -> Response {
    Response result;
    if (method != "GET"_str && method != "HEAD"_str) {
        result.head.status = lihttpto::StatusCode::make(u16(405)).unwrap();
        result.head.headers.add("allow"_str, "GET, HEAD"_str).unwrap();
        return result;
    }
    if (! metadata.exists) {
        result.head.status = lihttpto::StatusCode::make(u16(404)).unwrap();
        return result;
    }
    auto selected      = conditions.evaluate(method, metadata);
    result.head.status = selected.status();
    if (metadata.etag.is_some())
        headers.set("etag"_str, lihttpto::HeaderValue::make(metadata.etag->as_bytes()).unwrap())
            .unwrap();
    else
        headers.remove("etag"_str);
    auto modified = metadata.last_modified.is_some()
                        ? lihttpto::format_http_date(*metadata.last_modified)
                        : None<String>();
    if (modified.is_some())
        headers.set("last-modified"_str, modified->as_str()).unwrap();
    else
        headers.remove("last-modified"_str);
    if (metadata.accept_ranges)
        headers.set("accept-ranges"_str, "bytes"_str).unwrap();
    else
        headers.set("accept-ranges"_str, "none"_str).unwrap();
    auto range = selected.content_range(metadata.length);
    if (range.is_some())
        headers.set("content-range"_str, range->as_str()).unwrap();
    else
        headers.remove("content-range"_str);
    result.head.headers = rstd::move(headers);
    using Kind          = lihttpto::RepresentationKind;
    if (selected.kind == Kind::NotModified) {
        result.content_length = None();
        result.head.headers.remove("content-type"_str);
        result.head.headers.remove("content-encoding"_str);
    } else if (selected.kind == Kind::Full || selected.kind == Kind::Partial) {
        result.content_length = Some(selected.range.length);
        if (method == "GET"_str && selected.range.length != u64())
            result.body = body_factory(selected.range);
    }
    return result;
}
} // namespace liserveto
