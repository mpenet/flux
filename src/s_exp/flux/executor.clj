(ns s-exp.flux.executor
  "Wraps BlockingAdaptiveExecutor — an java.util.concurrent.Executor whose
  thread-pool size is governed by an adaptive concurrency limiter.

  Submitted tasks block the calling thread until a slot is available. The
  limiter observes each task's outcome:
    - success  when the Runnable returns normally
    - dropped  when it throws UncheckedTimeoutException or RejectedExecutionException
    - ignore   on any other exception

  Operations submitted should be homogeneous with similar latency profiles;
  the RTT feedback only comes from tasks that succeed."
  (:import
   (com.netflix.concurrency.limits.executors BlockingAdaptiveExecutor
                                             BlockingAdaptiveExecutor$Builder
                                             UncheckedTimeoutException)
   (java.util.concurrent Executor RejectedExecutionException)))

(set! *warn-on-reflection* true)

(defn blocking-adaptive-executor
  "Creates a BlockingAdaptiveExecutor.

  The executor's concurrency is governed by a limiter. When all slots are
  taken, `execute!` blocks the calling thread until one is released.

  Options:
    :limiter   a `Limiter<Void>` instance (e.g. from `s-exp.flux/simple-limiter`
               or `s-exp.flux/blocking-limiter`). When omitted, defaults to a
               SimpleLimiter with AIMDLimit.
    :executor  a `java.util.concurrent.Executor` for running submitted tasks.
               When omitted, defaults to a cached daemon-thread pool.
    :name      (string) identifier used in metrics."
  ^BlockingAdaptiveExecutor
  [{:keys [^Executor executor limiter name]}]
  (let [^BlockingAdaptiveExecutor$Builder b (BlockingAdaptiveExecutor/newBuilder)]
    (when limiter (.limiter b limiter))
    (when executor (.executor b executor))
    (when name (.name b name))
    (.build b)))

(defn execute!
  "Submits `f` (a zero-arg fn) to `executor`.

  Blocks the calling thread until a concurrency slot is available, then
  dispatches `f` to the underlying thread pool. Returns immediately once
  the task has been handed off.

  Throws `RejectedExecutionException` if the limiter cannot acquire a slot
  (which should not happen with the default blocking limiter, but may occur
  with a custom non-blocking one)."
  [^BlockingAdaptiveExecutor executor f]
  (.execute executor ^Runnable f))

(defn unchecked-timeout-exception
  "Creates an UncheckedTimeoutException.

  Throw this from inside a task submitted to a `blocking-adaptive-executor`
  to signal that the operation timed out or an external limit was hit.
  The limiter will record the outcome as `:dropped`, which causes loss-based
  algorithms (e.g. AIMD) to reduce the concurrency limit."
  (^UncheckedTimeoutException []
   (UncheckedTimeoutException.))
  (^UncheckedTimeoutException [^String message]
   (UncheckedTimeoutException. message))
  (^UncheckedTimeoutException [^String message ^Throwable cause]
   (UncheckedTimeoutException. message cause)))
