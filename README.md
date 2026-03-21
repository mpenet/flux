# flux

Clojure wrapper for [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits) — adaptive concurrency control based on TCP congestion algorithms.

Rather than setting a fixed request-per-second cap, flux dynamically adjusts the number of allowed in-flight operations based on observed latency and error signals. The limit rises when things are going well and backs off when the system shows signs of saturation.

## Installation

```clojure
;; deps.edn
{com.s-exp/flux {:mvn/version "0.1.0"}}
```

## Concepts

### Limit algorithms

A *limit* is the algorithm that decides the current concurrency ceiling. It receives timing and error feedback after every operation and adjusts accordingly.

| Algorithm | Strategy | Good for |
|-----------|----------|----------|
| `vegas-limit` | Delay-based — watches queue build-up via RTT deviation | General server-side use |
| `gradient2-limit` | Tracks divergence between short and long RTT averages | Services with variable baseline latency |
| `aimd-limit` | Additive increase / multiplicative decrease on drop signals | Client-side, loss-driven environments |
| `fixed-limit` | Static ceiling, never adapts | Testing, or when you want a simple semaphore |

### Limiter

A *limiter* wraps a limit algorithm and enforces it. `simple-limiter` is the standard choice: it maintains an atomic in-flight counter and rejects requests (returns `nil` from `acquire!`) when the counter reaches the current limit.

### Listener lifecycle

Every successful `acquire!` returns a *listener*. You **must** call exactly one of three functions on it when the operation completes — this is how the algorithm learns:

| Function | When to use |
|----------|-------------|
| `success!` | Operation completed normally; RTT is recorded |
| `ignore!` | Operation failed for reasons unrelated to capacity (e.g. auth error, validation failure) — RTT is discarded |
| `dropped!` | Operation was rejected downstream or timed out — signals degradation; loss-based algorithms reduce the limit aggressively |

Forgetting to call one of these leaks a slot permanently.

## Usage

### Basic setup

```clojure
(require '[s-exp.flux :as flux])

;; 1. Choose a limit algorithm
(def limit (flux/vegas-limit {:max-concurrency 200}))

;; 2. Create a limiter
(def limiter (flux/simple-limiter limit))
```

### Low-level API

`acquire!` returns a listener on success, or `nil` if the limit is currently exceeded.

```clojure
(if-let [listener (flux/acquire! limiter)]
  (try
    (let [result (do-work)]
      (flux/success! listener)
      result)
    (catch Throwable t
      (flux/dropped! listener)
      (throw t)))
  (handle-rejection))
```

`acquire!` accepts an optional context value passed through to the limit algorithm (useful for partitioned limiters):

```clojure
(flux/acquire! limiter {:user-id "abc123"})
```

### High-level API: `attempt!`

`attempt!` manages the acquire/signal lifecycle automatically:

```clojure
;; Simple case — calls (do-work), signals success on return, dropped on exception
(flux/attempt! limiter do-work)

;; With options
(flux/attempt! limiter do-work
  {:on-reject #(throw (ex-info "Too busy" {:status 503}))
   :classify  (fn [result]
                (if (:error result) :dropped :success))
   :context   {:tenant "acme"}})
```

Options for `attempt!`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `:context` | any | `nil` | Passed to `acquire!` |
| `:on-reject` | `(fn [])` | throws `ex-info` | Called when limit is exceeded |
| `:classify` | `(fn [result])` | always `:success` | Maps the return value to `:success`, `:ignore`, or `:dropped` |

On exception, `attempt!` always signals `:dropped` and rethrows.

The default rejection throws:
```clojure
(ex-info "Concurrency limit exceeded" {:type :s-exp.flux/limit-exceeded})
```

### Limit algorithm options

#### `vegas-limit`

Delay-based algorithm. Estimates queue size from the ratio of minimum observed RTT to current RTT. A good default for most server-side use cases.

```clojure
(flux/vegas-limit
  {:initial-limit    20    ; starting concurrency
   :max-concurrency  1000  ; hard ceiling
   :smoothing        1.0   ; 0.0–1.0, lower = slower adaptation
   :probe-multiplier 30})  ; how often to probe for a new RTT baseline
```

#### `gradient2-limit`

Tracks short-term vs long-term RTT gradient. More stable than Vegas under bursty load, at the cost of slower reaction.

```clojure
(flux/gradient2-limit
  {:initial-limit   20
   :min-limit       20     ; floor — never goes below this
   :max-concurrency 200
   :smoothing       0.2    ; lower = more stable, slower to adapt
   :rtt-tolerance   1.5    ; allow RTT to grow this much before backing off
   :long-window     600    ; ms, baseline RTT measurement window
   :queue-size      4})    ; extra buffer slots above the estimated limit
```

#### `aimd-limit`

Classic AIMD: increments the limit by 1 on success, multiplies down by `backoff-ratio` on a drop signal or timeout. Predictable behaviour, works well when the drop signal is clear.

```clojure
(flux/aimd-limit
  {:initial-limit 20
   :min-limit     20
   :max-limit     200
   :backoff-ratio 0.9          ; 0.5–1.0, how aggressively to back off
   :timeout-ns    5000000000}) ; 5s in nanoseconds — treat slow calls as drops
```

#### `fixed-limit`

Non-adaptive. Useful for testing or as a simple counting semaphore.

```clojure
(flux/fixed-limit {:limit 50})
```

### Limiter constructors

#### `simple-limiter`

The standard limiter. Immediately rejects (`acquire!` returns `nil`) when the in-flight count reaches the current limit.

```clojure
(flux/simple-limiter (flux/vegas-limit {}))
(flux/simple-limiter (flux/vegas-limit {}) {:name "my-service"}) ; :name is optional, used for metrics
```

#### `blocking-limiter`

Wraps any limiter. Instead of rejecting when the limit is reached, the calling thread **blocks** until a slot becomes free or the timeout expires. On timeout or interrupt, `acquire!` returns `nil`.

Useful for batch clients, background workers, or any context where queuing up behind backpressure is preferable to fast-failing.

```clojure
(def limiter
  (flux/blocking-limiter
    (flux/simple-limiter (flux/vegas-limit {}))
    {:timeout-ms 5000})) ; block for up to 5 seconds, then return nil
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:timeout-ms` | long | 1 hour | Max time to block waiting for a slot |

#### `lifo-blocking-limiter`

Like `blocking-limiter` but queues waiting threads in **last-in, first-out** order. When a slot opens up, the most recently queued thread is unblocked first. This means the oldest queued requests time out and shed load first, keeping the queue fresh and favouring availability over tail latency.

The backlog has a bounded size. When it fills up, further requests are rejected immediately rather than queuing.

```clojure
(flux/lifo-blocking-limiter
  (flux/simple-limiter (flux/vegas-limit {}))
  {:backlog-size       100   ; max threads waiting; excess rejected immediately
   :backlog-timeout-ms 1000  ; how long a queued thread waits before giving up
   })
```

The backlog timeout can also be derived dynamically from the acquire context, which lets you implement per-tenant or per-priority timeouts:

```clojure
(flux/lifo-blocking-limiter
  (flux/simple-limiter (flux/vegas-limit {}))
  {:backlog-timeout-fn (fn [ctx] (get ctx :timeout-ms 500))})
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:backlog-size` | int | 100 | Max queued threads; excess are rejected immediately |
| `:backlog-timeout-ms` | long | 1000 | Fixed timeout in ms for queued threads |
| `:backlog-timeout-fn` | `(fn [ctx] -> long ms)` | `nil` | Dynamic timeout derived from context; overrides `:backlog-timeout-ms` |

#### `partitioned-limiter`

Wraps any `AbstractLimiter` and divides its capacity among named partitions according to fixed ratios. Each partition gets a guaranteed slice of the adaptive limit:

```
partition-budget = floor(current-total-limit × ratio)
```

Requests that resolve to a known partition are admitted only when that partition's in-flight count is below its budget. Requests that resolve to an unknown or `nil` key use the leftover **overflow** capacity — the portion not allocated to any named partition. The underlying limiter still enforces the global ceiling; partitioning is a pure admission gate on top.

```clojure
(def limiter
  (flux/partitioned-limiter
    (flux/simple-limiter (flux/vegas-limit {:max-concurrency 100}))
    (fn [ctx] (get-in ctx [:headers "x-tier"]))
    {"live"  0.8
     "batch" 0.1}))
     ; remaining 0.1 is overflow capacity for unrecognised tier values
```

`acquire!` and `attempt!` work exactly as usual — pass the context that `partition-by` expects:

```clojure
(flux/acquire! limiter {"x-tier" "live"})

(flux/attempt! limiter do-work :context {"x-tier" "batch"})
```

| Argument | Type | Description |
|----------|------|-------------|
| `limiter` | `AbstractLimiter` | The underlying limiter (e.g. from `simple-limiter`) |
| `partition-by` | `(fn [context] -> key \| nil)` | Extracts a partition key from the acquire context |
| `partitions` | `{key double}` | Map of partition key → ratio (0.0–1.0); ratios should sum to ≤ 1.0 |

With the Ring middleware, supply a `:context-fn` that returns whatever `partition-by` expects:

```clojure
(flux.ring/wrap-concurrency-limit handler
  (flux/partitioned-limiter
    (flux/simple-limiter (flux/vegas-limit {}))
    :tier   ; keyword lookup on the context map
    {:live  0.8
     :batch 0.1})
  {:context-fn (fn [req] {:tier (keyword (get-in req [:headers "x-tier"]))})})
```

## Ring middleware

`s-exp.flux.ring/wrap-concurrency-limit` integrates with any Ring-compatible stack (Jetty, http-kit, Pedestal, etc.) and supports both the synchronous `[request]` and asynchronous `[request respond raise]` Ring arities.

### Basic usage

```clojure
(require '[s-exp.flux :as flux]
         '[s-exp.flux.ring :as flux.ring])

(def limiter
  (flux/simple-limiter (flux/vegas-limit {:max-concurrency 200})))

(def app
  (-> your-handler
      (flux.ring/wrap-concurrency-limit limiter)))
```

When the limit is exceeded the middleware returns a 503 by default:

```
HTTP/1.1 503 Service Unavailable
Content-Type: text/plain
Retry-After: 1

Service temporarily unavailable - concurrency limit exceeded
```

### Default classify behaviour

The middleware maps Ring response status codes to limiter signals automatically:

| Status | Signal | Rationale |
|--------|--------|-----------|
| 5xx (except 503) | `:dropped` | Server errors indicate capacity problems |
| 503 | `:success` | This is the middleware's own backpressure response — not a signal from the app |
| everything else | `:success` | Normal outcomes |

### Options

```clojure
(flux.ring/wrap-concurrency-limit handler limiter
  {:context-fn (fn [request]
                 ;; extract whatever you want to pass as limiter context
                 (get-in request [:headers "x-tenant-id"]))

   :on-reject  (fn [request]
                 {:status  429
                  :headers {"Content-Type" "text/plain"
                            "Retry-After"  "1"}
                  :body    "Too many requests"})

   :classify   (fn [response]
                 ;; must return :success, :ignore, or :dropped
                 (cond
                   (< (:status response) 500) :success
                   (= (:status response) 503) :ignore
                   :else                      :dropped))

   :on-error   (fn [request throwable]
                 ;; return a response map to handle the error gracefully,
                 ;; return nil to let the exception propagate
                 {:status 500 :body "Internal error"})})
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:context-fn` | `(fn [request])` | `identity` | Extracts limiter context from the request |
| `:on-reject` | `(fn [request])` | 503 response | Called when the limit is exceeded |
| `:classify` | `(fn [response])` | 5xx→`:dropped`, rest→`:success` | Maps a Ring response to a limiter signal |
| `:on-error` | `(fn [request throwable])` | `nil` (rethrow) | Called when the handler throws; return a response to swallow the error |

## General-purpose use

The core API is not Ring-specific. You can use it to guard anything: outbound HTTP calls, database queries, queue consumers, background job slots, etc.

```clojure
;; Guard outgoing HTTP calls with AIMD backoff on 5xx
(def http-limiter
  (flux/simple-limiter (flux/aimd-limit {:initial-limit 10
                                         :max-limit      50})))

(defn fetch! [url]
  (flux/attempt! http-limiter
    #(http/get url)
    {:classify  (fn [resp]
                  (if (>= (:status resp) 500) :dropped :success))
     :on-reject #(throw (ex-info "HTTP client saturated" {:url url}))}))
```

```clojure
;; Guard a queue consumer
;; — use :ignore on empty poll so idle time doesn't skew the RTT baseline
(flux/attempt! queue-limiter
  (fn []
    (if-let [msg (queue/poll! q {:timeout 100})]
      (process! msg)
      ::empty))
  {:classify (fn [result]
               (if (= result ::empty) :ignore :success))})
```

## BlockingAdaptiveExecutor

`s-exp.flux.executor` wraps `BlockingAdaptiveExecutor` — a `java.util.concurrent.Executor` whose thread-pool size is governed by an adaptive concurrency limiter.

When all slots are taken, `execute!` **blocks the calling thread** until one is released. Tasks are dispatched to an underlying thread pool. The limiter observes each task's outcome:

- Returns normally → `:success` (RTT recorded)
- Throws `UncheckedTimeoutException` → `:dropped` (limit backs off)
- Throws anything else → `:ignore` (RTT discarded)

Best suited for batch workloads and background pipelines where the work is homogeneous and you want the thread pool size to track the system's actual capacity.

```clojure
(require '[s-exp.flux :as flux]
         '[s-exp.flux.executor :as flux.executor])

(def executor
  (flux.executor/blocking-adaptive-executor
    {:limiter (flux/simple-limiter (flux/vegas-limit {:max-concurrency 50}))
     :name    "my-batch-pool"}))

;; Blocks until a slot is free, then hands off to the thread pool
(flux.executor/execute! executor
  (fn []
    (process-item! item)))
```

When a task times out or hits an external limit, throw `UncheckedTimeoutException` to signal `:dropped` to the limiter:

```clojure
(flux.executor/execute! executor
  (fn []
    (let [result (deref (http/get url) 500 ::timeout)]
      (when (= ::timeout result)
        (throw (flux.executor/unchecked-timeout-exception "upstream timeout")))
      (process! result))))
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:limiter` | `Limiter<Void>` | SimpleLimiter + AIMDLimit | Controls concurrency |
| `:executor` | `java.util.concurrent.Executor` | cached daemon-thread pool | Runs submitted tasks |
| `:name` | string | auto-generated | Identifier for metrics |

## License

Copyright © 2026 Max Penet

Distributed under the Eclipse Public License version 1.0.
