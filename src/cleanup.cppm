export module liserveto:cleanup;
export import rstd;
using namespace rstd::prelude;

struct ConnectionCleanupPoll {
    template<class Self, class Delegate = void>
    struct Api {
        using Trait = ConnectionCleanupPoll;
        auto poll(rstd::task::Context& cx) -> rstd::task::Poll<empty> {
            return rstd::trait_call<0>(this, cx);
        }
    };
    template<class S>
    using Funcs = TraitFuncs<&S::poll>;
};

template<class F>
struct StoredConnectionCleanup {
    F    future;
    auto poll(rstd::task::Context& cx) -> rstd::task::Poll<empty> {
        return rstd::future::poll(future, cx);
    }
};
struct ConnectionCleanupJob {
    using Output = empty;
    Box<dyn<ConnectionCleanupPoll>> future;
    auto poll(mut_ref<ConnectionCleanupJob> self, rstd::task::Context& cx)
        -> rstd::task::Poll<empty> {
        return self->future->poll(cx);
    }
};
struct ConnectionCleanup {
    struct Finished {
        template<class Self, class Delegate = void>
        struct Api {
            using Trait = Finished;
            void run() { rstd::trait_call<0>(this); }
        };
        template<class S>
        using Funcs = TraitFuncs<&S::run>;
    };
    template<class F>
    struct StoredFinished {
        F    function;
        void run() { function(); }
    };
    using Jobs      = Vec<ConnectionCleanupJob>;
    using Callbacks = Vec<Box<dyn<Finished>>>;
    struct Fields {
        Jobs      jobs;
        Callbacks callbacks;
        bool      polling {};
    };
    rstd::sync::Mutex<Fields>                          fields { Fields {} };
    Option<rstd::async::JoinAll<ConnectionCleanupJob>> waiting;
    template<class F>
    void defer(F future) {
        fields.lock().unwrap()->jobs.push(
            ConnectionCleanupJob { Box<dyn<ConnectionCleanupPoll>>::make(
                StoredConnectionCleanup<F> { rstd::move(future) }) });
    }
    template<class F>
    void complete(F function) {
        auto callback = Box<dyn<Finished>>::make(StoredFinished<F> { rstd::move(function) });
        {
            auto locked = fields.lock().unwrap();
            if (locked->polling || ! locked->jobs.is_empty()) {
                locked->callbacks.push(rstd::move(callback));
                return;
            }
        }
        callback->run();
    }
    auto poll(rstd::task::Context& cx) -> rstd::task::Poll<empty> {
        Callbacks callbacks;
        for (;;) {
            if (waiting.is_none()) {
                auto locked     = fields.lock().unwrap();
                waiting         = Some(rstd::async::join_all(rstd::move(locked->jobs)));
                locked->polling = true;
            }
            if (rstd::future::poll(*waiting, cx).is_pending())
                return rstd::task::Poll<empty>::Pending();
            waiting     = None();
            auto locked = fields.lock().unwrap();
            if (! locked->jobs.is_empty()) continue;
            locked->polling = false;
            callbacks       = rstd::move(locked->callbacks);
            break;
        }
        for (auto& callback : callbacks) callback->run();
        return rstd::task::Poll<empty>::Ready(empty {});
    }
};

struct ConnectionCleanupWait {
    using Output = empty;
    rstd::sync::Arc<ConnectionCleanup> owner;
    auto poll(mut_ref<ConnectionCleanupWait> self, rstd::task::Context& cx)
        -> rstd::task::Poll<empty> {
        return self->owner->poll(cx);
    }
};
