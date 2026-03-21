(ns s-exp.flux
  "Clojure wrapper over Netflix concurrency-limits.

  Core concepts:
  - A `limiter` controls inflight concurrency using an adaptive algorithm.
  - Calling `acquire!` attempts to get a token; returns a listener map on success, nil on rejection.
  - The listener must be finalized with `success!`, `ignore!`, or `dropped!`.
  - `with-limit` handles the acquire/finalize lifecycle automatically."
  (:import
   (com.netflix.concurrency.limits Limiter Limiter$Listener)
   (com.netflix.concurrency.limits.limit AIMDLimit FixedLimit Gradient2Limit Gradient2Limit$Builder VegasLimit)
   (com.netflix.concurrency.limits.limiter BlockingLimiter LifoBlockingLimiter LifoBlockingLimiter$Builder SimpleLimiter SimpleLimiter$Builder)
   (java.time Duration)))

(set! *warn-on-reflection* true)
;;; Limit algorithms

(defn vegas-limit
  "Creates a VegasLimit - a delay-based adaptive algorithm.

  Options:
    :initial-limit    (int, default 20)
    :max-concurrency  (int, default 1000)
    :smoothing        (double 0.0-1.0, default 1.0)
    :probe-multiplier (int, default 30)"
  ^VegasLimit
  [{:keys [initial-limit max-concurrency smoothing probe-multiplier]}]
  (cond-> (VegasLimit/newBuilder)
    initial-limit (.initialLimit initial-limit)
    max-concurrency (.maxConcurrency max-concurrency)
    smoothing (.smoothing smoothing)
    probe-multiplier (.probeMultiplier probe-multiplier)
    true (.build)))

(defn gradient2-limit
  "Creates a Gradient2Limit - tracks divergence between exponential averages.

  Options:
    :initial-limit    (int, default 20)
    :min-limit        (int, default 20)
    :max-concurrency  (int, default 200)
    :smoothing        (double 0.0-1.0, default 0.2)
    :rtt-tolerance    (double >= 1.0, default 1.5)
    :long-window      (int ms, default 600)
    :queue-size       (int)"
  ^Gradient2Limit
  [{:keys [initial-limit min-limit max-concurrency smoothing rtt-tolerance long-window queue-size]}]
  (let [^Gradient2Limit$Builder b (Gradient2Limit/newBuilder)]
    (when initial-limit (.initialLimit b initial-limit))
    (when min-limit (.minLimit b min-limit))
    (when max-concurrency (.maxConcurrency b max-concurrency))
    (when smoothing (.smoothing b smoothing))
    (when rtt-tolerance (.rttTolerance b rtt-tolerance))
    (when long-window (.longWindow b long-window))
    (when queue-size (.queueSize b (int queue-size)))
    (.build b)))

(defn aimd-limit
  "Creates an AIMDLimit - additive increase / multiplicative decrease.
  Good for client-side limiting or when drop events are the signal.

  Options:
    :initial-limit  (int, default 20)
    :min-limit      (int, default 20)
    :max-limit      (int, default 200)
    :backoff-ratio  (double 0.5-1.0, default 0.9)
    :timeout-ns     (long nanoseconds)"
  ^AIMDLimit
  [{:keys [initial-limit min-limit max-limit backoff-ratio timeout-ns]}]
  (cond-> (AIMDLimit/newBuilder)
    initial-limit (.initialLimit initial-limit)
    min-limit (.minLimit min-limit)
    max-limit (.maxLimit max-limit)
    backoff-ratio (.backoffRatio backoff-ratio)
    timeout-ns (.timeout timeout-ns java.util.concurrent.TimeUnit/NANOSECONDS)
    true (.build)))

(defn fixed-limit
  "Creates a FixedLimit - non-adaptive, static concurrency cap.

  Options:
    :limit  (int, required)"
  ^FixedLimit
  [{:keys [limit]}]
  (FixedLimit/of limit))

;;; Limiter construction

(defn simple-limiter
  "Creates a SimpleLimiter wrapping a limit algorithm.

  `limit` is a limit instance (vegas-limit, gradient2-limit, aimd-limit, fixed-limit).

  Options:
    :name  (string) name for metrics"
  ^SimpleLimiter
  [limit & {:keys [name]}]
  (let [^SimpleLimiter$Builder b (SimpleLimiter/newBuilder)]
    (.limit b limit)
    (when name (.named b name))
    (.build b)))

(defn blocking-limiter
  "Wraps any limiter to block the calling thread when the limit is reached,
  rather than rejecting immediately.

  The thread waits until a slot becomes available or the timeout expires.
  On timeout or interrupt, `acquire!` returns nil.

  Options:
    :timeout-ms  (long) maximum time to block in milliseconds.
                 Defaults to 1 hour. Must be less than 1 hour."
  ^BlockingLimiter
  [^Limiter limiter & {:keys [timeout-ms]}]
  (if timeout-ms
    (BlockingLimiter/wrap limiter (Duration/ofMillis timeout-ms))
    (BlockingLimiter/wrap limiter)))

(defn lifo-blocking-limiter
  "Wraps any limiter with LIFO (last-in, first-out) blocking semantics.

  When the limit is reached, incoming threads are queued. The most recently
  queued thread is unblocked first, which favours availability over latency:
  the oldest waiting requests shed first, keeping the queue fresh.

  Options:
    :backlog-size         (int) maximum number of threads that may block waiting;
                          excess requests are rejected immediately. Default: 100.
    :backlog-timeout-ms   (long) fixed timeout in milliseconds for queued threads.
                          Default: 1000 ms.
    :backlog-timeout-fn   (fn [context] -> long ms) derives the timeout
                          dynamically from the acquire context. When provided,
                          takes precedence over :backlog-timeout-ms."
  ^LifoBlockingLimiter
  [^Limiter limiter & {:keys [backlog-size backlog-timeout-ms backlog-timeout-fn]}]
  (let [^LifoBlockingLimiter$Builder b (LifoBlockingLimiter/newBuilder limiter)]
    (when backlog-size (.backlogSize b (int backlog-size)))
    (when backlog-timeout-fn
      (.backlogTimeout b
                       (reify java.util.function.Function
                         (apply [_ ctx] (backlog-timeout-fn ctx)))
                       java.util.concurrent.TimeUnit/MILLISECONDS))
    (when (and backlog-timeout-ms (not backlog-timeout-fn))
      (.backlogTimeoutMillis b backlog-timeout-ms))
    (.build b)))

;;; Token / listener protocol

(defn acquire!
  "Attempts to acquire a concurrency token from `limiter`.

  Returns a `Limiter$Listener` on success, or nil if the limit is exceeded.

  You MUST call one of `success!`, `ignore!`, or `dropped!` on the
  returned listener when the work completes."
  (^Limiter$Listener [^Limiter limiter]
   (acquire! limiter nil))
  (^Limiter$Listener [^Limiter limiter context]
   (.orElse (.acquire limiter context) nil)))

(defn success!
  "Signal that the guarded operation completed successfully.
  The measured latency will be used to tune the limit algorithm."
  [^Limiter$Listener listener]
  (.onSuccess listener))

(defn ignore!
  "Signal that the operation failed before producing meaningful timing data
  (e.g. validation error, auth failure). The RTT sample is discarded so it
  does not skew the algorithm."
  [^Limiter$Listener listener]
  (.onIgnore listener))

(defn dropped!
  "Signal that the request was dropped externally (timeout, upstream rejection).
  Loss-based algorithms will typically respond with a limit reduction."
  [^Limiter$Listener listener]
  (.onDropped listener))

;;; High-level helper

(defn attempt!
  "Acquires a token from `limiter` and calls `f`.

  Options:
    :context    Passed to the limiter's acquire; defaults to nil.

    :on-reject  (fn [] -> any) called when the limit is exceeded.
                Defaults to throwing an ex-info with :type ::s-exp.flux/limit-exceeded.

    :classify   (fn [result] -> :success | :ignore | :dropped)
                Maps the return value of `f` to a limiter signal.
                Defaults to always :success.

  Returns the return value of `f` (or `on-reject`) on success, or throws if
  rejected and no `on-reject` is provided."
  ([limiter f & {:keys [context on-reject classify]
                 :or {classify (constantly :success)}}]
   (if-let [listener (acquire! limiter context)]
     (try
       (let [result (f)]
         (case (classify result)
           :success (success! listener)
           :ignore (ignore! listener)
           :dropped (dropped! listener))
         result)
       (catch Throwable t
         (dropped! listener)
         (throw t)))
     (if on-reject
       (on-reject)
       (throw (ex-info "Concurrency limit exceeded" {:type :s-exp.flux/limit-exceeded}))))))
