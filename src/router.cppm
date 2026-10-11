export module liserveto:router;
export import :endpoint;

using namespace rstd::prelude;
using namespace rstd::literals;

enum class RoutePartKind
{
    Literal,
    Parameter,
    CatchAll
};
struct RoutePart {
    RoutePartKind kind;
    String        value;
};
struct RegisteredRoute {
    Vec<RoutePart>      parts;
    Option<String>      method;
    liserveto::Endpoint endpoint;
    bool                prefix {};
};

export namespace liserveto
{
enum class RouteError
{
    InvalidPattern,
    InvalidMethod,
    Duplicate,
    ParameterConflict
};
}

auto parse_route_pattern(ref<str> pattern) -> Result<Vec<RoutePart>, liserveto::RouteError> {
    using liserveto::RouteError;
    if (! pattern.starts_with("/"_str)) return Err(RouteError::InvalidPattern);
    Vec<RoutePart> parts;
    if (pattern == "/"_str) return Ok(rstd::move(parts));
    usize start(1);
    for (;;) {
        auto end = start;
        while (end < pattern.len() && pattern.as_bytes()[end] != u8('/')) ++end;
        auto text  = pattern.get(start, end).unwrap();
        auto kind  = RoutePartKind::Literal;
        auto value = text;
        if (text.starts_with("{"_str) && text.ends_with("}"_str)) {
            value = text.get(usize(1), text.len() - usize(1)).unwrap();
            kind  = RoutePartKind::Parameter;
            if (value.starts_with("*"_str)) {
                kind  = RoutePartKind::CatchAll;
                value = value.get(usize(1), value.len()).unwrap();
                if (end != pattern.len()) return Err(RouteError::InvalidPattern);
            }
            if (value.is_empty()) return Err(RouteError::InvalidPattern);
            for (auto byte : value.as_bytes())
                if (! rstd::ascii::is_alnum(byte) && byte != u8('_'))
                    return Err(RouteError::InvalidPattern);
            for (const auto& part : parts)
                if (part.kind != RoutePartKind::Literal && part.value == value)
                    return Err(RouteError::ParameterConflict);
        } else
            for (auto byte : text.as_bytes())
                if (byte == u8('{') || byte == u8('}') || byte == u8('?') || byte == u8('#') ||
                    byte == u8('%'))
                    return Err(RouteError::InvalidPattern);
        parts.push({ kind, String::make(value) });
        if (end == pattern.len()) break;
        start = end + usize(1);
    }
    return Ok(rstd::move(parts));
}
auto route_matches(const RegisteredRoute& route, const liserveto::Request& request) -> bool {
    auto form = request.head().line.resource.form;
    if (form != lihttpto::TargetForm::Origin && form != lihttpto::TargetForm::Absolute)
        return false;
    usize index {};
    for (const auto& part : route.parts) {
        if (part.kind == RoutePartKind::CatchAll) return true;
        if (index == request.path_size()) return false;
        if (part.kind == RoutePartKind::Literal &&
            part.value.as_str().as_bytes() != request.path_segment(index))
            return false;
        ++index;
    }
    return route.prefix || index == request.path_size();
}
auto route_precedence(const RegisteredRoute& left, const RegisteredRoute& right) -> int {
    auto length = rstd::cmp::max(left.parts.len(), right.parts.len()) + usize(1);
    auto rank   = [](const RegisteredRoute& route, usize index) {
        if (index >= route.parts.len()) return route.prefix ? 0 : 4;
        switch (route.parts[index].kind) {
        case RoutePartKind::Literal: return 3;
        case RoutePartKind::Parameter: return 2;
        case RoutePartKind::CatchAll: return 1;
        }
        return 0;
    };
    for (usize index {}; index < length; ++index) {
        auto difference = rank(left, index) - rank(right, index);
        if (difference != 0) return difference;
    }
    return 0;
}

export namespace liserveto
{
class Router {
    Vec<RegisteredRoute> routes_;
    Option<Endpoint>     fallback_;
    auto insert(ref<str> pattern, Option<String> method, Endpoint endpoint, bool prefix)
        -> Result<empty, RouteError> {
        auto parsed = parse_route_pattern(pattern);
        if (parsed.is_err()) return Err(parsed.unwrap_err());
        auto parts = rstd::move(parsed).unwrap();
        if (prefix)
            for (const auto& part : parts)
                if (part.kind != RoutePartKind::Literal) return Err(RouteError::InvalidPattern);
        for (const auto& route : routes_) {
            bool same   = true;
            auto common = rstd::min(route.parts.len(), parts.len());
            for (usize i {}; i < common; ++i) {
                if (route.parts[i].kind != parts[i].kind ||
                    (parts[i].kind == RoutePartKind::Literal &&
                     route.parts[i].value != parts[i].value)) {
                    same = false;
                    break;
                }
                if (parts[i].kind != RoutePartKind::Literal &&
                    route.parts[i].value != parts[i].value)
                    return Err(RouteError::ParameterConflict);
            }
            if (same && route.parts.len() == parts.len() && route.prefix == prefix &&
                route.method == method)
                return Err(RouteError::Duplicate);
        }
        routes_.push({ rstd::move(parts), rstd::move(method), rstd::move(endpoint), prefix });
        return Ok(empty {});
    }

public:
    // Finish registration before sharing the router between concurrent calls.
    auto route(ref<str> method, ref<str> pattern, Endpoint endpoint) -> Result<empty, RouteError> {
        if (lihttpto::Method::parse(method).is_err()) return Err(RouteError::InvalidMethod);
        return insert(pattern, Some(String::make(method)), rstd::move(endpoint), false);
    }
    auto get(ref<str> pattern, Endpoint endpoint) -> Result<empty, RouteError> {
        return route("GET"_str, pattern, rstd::move(endpoint));
    }
    auto post(ref<str> pattern, Endpoint endpoint) -> Result<empty, RouteError> {
        return route("POST"_str, pattern, rstd::move(endpoint));
    }
    auto mount(ref<str> pattern, Endpoint endpoint) -> Result<empty, RouteError> {
        return insert(pattern, None(), rstd::move(endpoint), true);
    }
    auto nest(ref<str> pattern, Router router) -> Result<empty, RouteError> {
        return mount(pattern, rstd::move(router).into_endpoint());
    }
    void fallback(Endpoint endpoint) { fallback_ = Some(rstd::move(endpoint)); }
    auto into_endpoint() && -> Endpoint {
        return Endpoint::make(
            [router = rstd::move(*this)](Request request) -> rstd::async::coro<ResponseResult> {
                co_return co_await router.call(rstd::move(request));
            });
    }
    auto call(Request request) const -> rstd::async::coro<ResponseResult> {
        Option<usize> best;
        for (usize i {}; i < routes_.len(); ++i)
            if (route_matches(routes_[i], request) &&
                (best.is_none() || route_precedence(routes_[i], routes_[*best]) > 0))
                best = Some(i);
        if (best.is_none()) {
            if (fallback_.is_some()) co_return co_await fallback_->call(rstd::move(request));
            Response missing;
            missing.head.status = lihttpto::StatusCode::make(u16(404)).unwrap();
            co_return Ok(rstd::move(missing));
        }
        const auto&   shape  = routes_[*best];
        auto          method = request.head().line.method.as_str();
        Option<usize> selected;
        Option<usize> get;
        Vec<String>   allow;
        auto          add = [&](ref<str> value) {
            for (const auto& item : allow)
                if (item == value) return;
            allow.push(String::make(value));
        };
        for (usize i {}; i < routes_.len(); ++i) {
            const auto& route = routes_[i];
            if (! route_matches(route, request) || route_precedence(route, shape) != 0) continue;
            if (route.prefix) {
                selected = Some(i);
                break;
            }
            add(route.method->as_str());
            if (*route.method == "GET"_str) {
                get = Some(i);
                add("HEAD"_str);
            }
            if (*route.method == method) selected = Some(i);
        }
        if (selected.is_none() && method == "HEAD"_str) selected = get;
        if (selected.is_none()) {
            add("OPTIONS"_str);
            String methods;
            for (const auto& item : allow) {
                if (! methods.is_empty()) methods.push_str(", "_str);
                methods.push_str(item.as_str());
            }
            Response result;
            result.head.status =
                lihttpto::StatusCode::make(method == "OPTIONS"_str ? u16(204) : u16(405)).unwrap();
            result.head.headers.add("Allow"_str, methods.as_str()).unwrap();
            co_return Ok(rstd::move(result));
        }
        const auto& route = routes_[*selected];
        usize       index {};
        for (const auto& part : route.parts) {
            if (part.kind == RoutePartKind::Parameter)
                request.parameters_.push(
                    { part.value.clone(), Vec<u8>::from(request.path_segment(index)) });
            if (part.kind == RoutePartKind::CatchAll) {
                Vec<u8> bytes;
                for (usize i = index; i < request.path_size(); ++i) {
                    if (i != index) bytes.push(u8('/'));
                    for (auto byte : request.path_segment(i)) bytes.push(u8(byte));
                }
                request.parameters_.push({ part.value.clone(), rstd::move(bytes) });
                break;
            }
            ++index;
        }
        if (route.prefix) request.path_offset_ += index;
        co_return co_await route.endpoint.call(rstd::move(request));
    }
};
} // namespace liserveto
