(ns s-exp.flux-test
  (:require [clojure.test :refer [deftest is testing]]
            [s-exp.flux :as flux]
            [s-exp.flux.ring :as flux.ring]))

;;; Helpers

(defn- make-limiter
  ([] (make-limiter 10))
  ([max]
   (flux/simple-limiter (flux/fixed-limit {:limit max}))))

(defmacro with-acquired
  "Acquires a token from limiter, binds it to sym, runs body, always releases
  with success! to avoid slot leaks between tests."
  [[sym limiter] & body]
  `(let [~sym (flux/acquire! ~limiter nil)]
     (try
       ~@body
       (finally
         (when ~sym (flux/success! ~sym))))))

;;; Core API tests

(deftest acquire-and-release-test
  (testing "acquire returns a listener when under limit"
    (let [limiter (make-limiter 5)
          listener (flux/acquire! limiter nil)]
      (is (some? listener))
      (flux/success! listener)))

  (testing "acquire returns nil when limit is exhausted"
    (let [limiter (make-limiter 1)]
      (with-acquired [l1 limiter]
        (is (some? l1))
        (is (nil? (flux/acquire! limiter nil))))))

  (testing "slot is available again after success!"
    (let [limiter (make-limiter 1)
          l1 (flux/acquire! limiter nil)]
      (flux/success! l1)
      (let [l2 (flux/acquire! limiter nil)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "slot is available again after ignore!"
    (let [limiter (make-limiter 1)
          l1 (flux/acquire! limiter nil)]
      (flux/ignore! l1)
      (let [l2 (flux/acquire! limiter nil)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "slot is available again after dropped!"
    (let [limiter (make-limiter 1)
          l1 (flux/acquire! limiter nil)]
      (flux/dropped! l1)
      (let [l2 (flux/acquire! limiter nil)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "multiple slots can be held simultaneously"
    (let [limiter (make-limiter 3)
          l1 (flux/acquire! limiter nil)
          l2 (flux/acquire! limiter nil)
          l3 (flux/acquire! limiter nil)]
      (is (every? some? [l1 l2 l3]))
      (is (nil? (flux/acquire! limiter nil)))
      (flux/success! l1)
      (flux/success! l2)
      (flux/success! l3))))

;;; attempt! tests

(deftest attempt!-test
  (testing "returns value of f on success"
    (is (= :ok (flux/attempt! (make-limiter 5) (constantly :ok)))))

  (testing "two-arity (no opts) works"
    (is (= 42 (flux/attempt! (make-limiter 5) (constantly 42)))))

  (testing "throws ex-info with ::limit-exceeded on rejection by default"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"Concurrency limit exceeded"
                              (flux/attempt! limiter (constantly :ok))))
        (is (= :s-exp.flux/limit-exceeded
               (:type (ex-data (try (flux/attempt! limiter (constantly :ok))
                                    (catch clojure.lang.ExceptionInfo e e)))))))))

  (testing "calls on-reject fn and returns its value when provided"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (is (= :rejected
               (flux/attempt! limiter (constantly :ok)
                              {:on-reject (constantly :rejected)}))))))

  (testing "on-reject receives no args"
    (let [limiter (make-limiter 1)
          called (atom false)]
      (with-acquired [_ limiter]
        (flux/attempt! limiter (constantly :ok)
                       {:on-reject #(do (reset! called true) nil)})
        (is @called))))

  (testing "context is passed via opts"
    (let [limiter (make-limiter 5)]
      (is (= :ok (flux/attempt! limiter (constantly :ok) {:context ::my-ctx})))))

  (testing "classify :success signals success"
    (let [limiter (make-limiter 1)
          result (flux/attempt! limiter (constantly :val)
                                {:classify (constantly :success)})]
      (is (= :val result))
      (is (some? (flux/acquire! limiter)))))

  (testing "classify :ignore releases slot"
    (let [limiter (make-limiter 1)]
      (flux/attempt! limiter (constantly nil) {:classify (constantly :ignore)})
      (is (some? (flux/acquire! limiter)))))

  (testing "classify :dropped releases slot"
    (let [limiter (make-limiter 1)]
      (flux/attempt! limiter (constantly nil) {:classify (constantly :dropped)})
      (is (some? (flux/acquire! limiter)))))

  (testing "exception releases slot and rethrows"
    (let [limiter (make-limiter 1)]
      (is (thrown? RuntimeException
                   (flux/attempt! limiter #(throw (RuntimeException. "boom")))))
      (is (some? (flux/acquire! limiter))))))

;;; Limit algorithm constructors

(deftest vegas-limit-test
  (testing "creates a VegasLimit with defaults"
    (is (instance? com.netflix.concurrency.limits.limit.VegasLimit
                   (flux/vegas-limit {}))))
  (testing "accepts all options"
    (is (instance? com.netflix.concurrency.limits.limit.VegasLimit
                   (flux/vegas-limit {:initial-limit 10
                                      :max-concurrency 100
                                      :smoothing 0.5
                                      :probe-multiplier 20})))))

(deftest gradient2-limit-test
  (testing "creates a Gradient2Limit with defaults"
    (is (instance? com.netflix.concurrency.limits.limit.Gradient2Limit
                   (flux/gradient2-limit {}))))
  (testing "accepts all options"
    (is (instance? com.netflix.concurrency.limits.limit.Gradient2Limit
                   (flux/gradient2-limit {:initial-limit 10
                                          :min-limit 5
                                          :max-concurrency 100
                                          :smoothing 0.3
                                          :rtt-tolerance 2.0
                                          :long-window 300
                                          :queue-size 4})))))

(deftest aimd-limit-test
  (testing "creates an AIMDLimit with defaults"
    (is (instance? com.netflix.concurrency.limits.limit.AIMDLimit
                   (flux/aimd-limit {}))))
  (testing "accepts all options"
    (is (instance? com.netflix.concurrency.limits.limit.AIMDLimit
                   (flux/aimd-limit {:initial-limit 10
                                     :min-limit 5
                                     :max-limit 100
                                     :backoff-ratio 0.8
                                     :timeout-ns 1000000000})))))

(deftest fixed-limit-test
  (testing "creates a FixedLimit"
    (is (instance? com.netflix.concurrency.limits.limit.FixedLimit
                   (flux/fixed-limit {:limit 42})))))

(deftest simple-limiter-test
  (testing "creates a named limiter"
    (is (instance? com.netflix.concurrency.limits.limiter.SimpleLimiter
                   (flux/simple-limiter (flux/fixed-limit {:limit 5})
                                        {:name "my-limiter"})))))

(deftest blocking-limiter-test
  (testing "creates a BlockingLimiter with default timeout"
    (is (instance? com.netflix.concurrency.limits.limiter.BlockingLimiter
                   (flux/blocking-limiter (make-limiter 5)))))

  (testing "creates a BlockingLimiter with custom timeout"
    (is (instance? com.netflix.concurrency.limits.limiter.BlockingLimiter
                   (flux/blocking-limiter (make-limiter 5) {:timeout-ms 2000}))))

  (testing "acquires a slot when under limit"
    (let [limiter (flux/blocking-limiter (make-limiter 5))
          listener (flux/acquire! limiter)]
      (is (some? listener))
      (flux/success! listener)))

  (testing "blocks and acquires once a slot is released"
    (let [inner (make-limiter 1)
          limiter (flux/blocking-limiter inner {:timeout-ms 2000})
          l1 (flux/acquire! limiter)
          result (promise)]
      ;; release l1 from another thread after a short delay
      (future (Thread/sleep 50) (flux/success! l1))
      ;; this acquire should block then succeed
      (future (deliver result (flux/acquire! limiter)))
      (let [l2 (deref result 1000 ::timeout)]
        (is (not= ::timeout l2))
        (is (some? l2))
        (when (some? l2) (flux/success! l2)))))

  (testing "returns nil on timeout when limit is exhausted"
    (let [inner (make-limiter 1)
          limiter (flux/blocking-limiter inner {:timeout-ms 50})
          l1 (flux/acquire! limiter)]
      (try
        (is (nil? (flux/acquire! limiter)))
        (finally
          (flux/success! l1))))))

(deftest lifo-blocking-limiter-test
  (testing "creates a LifoBlockingLimiter with defaults"
    (is (instance? com.netflix.concurrency.limits.limiter.LifoBlockingLimiter
                   (flux/lifo-blocking-limiter (make-limiter 5)))))

  (testing "creates a LifoBlockingLimiter with all fixed options"
    (is (instance? com.netflix.concurrency.limits.limiter.LifoBlockingLimiter
                   (flux/lifo-blocking-limiter (make-limiter 5)
                                               {:backlog-size 50
                                                :backlog-timeout-ms 500}))))

  (testing "creates a LifoBlockingLimiter with dynamic timeout fn"
    (is (instance? com.netflix.concurrency.limits.limiter.LifoBlockingLimiter
                   (flux/lifo-blocking-limiter (make-limiter 5)
                                               {:backlog-timeout-fn (constantly 200)}))))

  (testing "acquires a slot when under limit"
    (let [limiter (flux/lifo-blocking-limiter (make-limiter 5))
          listener (flux/acquire! limiter)]
      (is (some? listener))
      (flux/success! listener)))

  (testing "blocks and acquires once a slot is released (LIFO)"
    (let [inner (make-limiter 1)
          limiter (flux/lifo-blocking-limiter inner {:backlog-timeout-ms 2000})
          l1 (flux/acquire! limiter)
          result (promise)]
      (future (Thread/sleep 50) (flux/success! l1))
      (future (deliver result (flux/acquire! limiter)))
      (let [l2 (deref result 1000 ::timeout)]
        (is (not= ::timeout l2))
        (is (some? l2))
        (when (some? l2) (flux/success! l2)))))

  (testing "returns nil when backlog timeout expires"
    (let [inner (make-limiter 1)
          limiter (flux/lifo-blocking-limiter inner {:backlog-timeout-ms 50})
          l1 (flux/acquire! limiter)]
      (try
        (is (nil? (flux/acquire! limiter)))
        (finally
          (flux/success! l1)))))

  (testing "rejects immediately when backlog is full"
    (let [inner (make-limiter 1)
          limiter (flux/lifo-blocking-limiter inner {:backlog-size 1
                                                     :backlog-timeout-ms 2000})
          l1 (flux/acquire! limiter)
          ;; fill the single backlog slot
          _blocking (future (flux/acquire! limiter))]
      (Thread/sleep 20) ; let the future enter the backlog
      (try
        ;; backlog is full — should reject immediately
        (is (nil? (flux/acquire! limiter)))
        (finally
          (flux/success! l1))))))

;;; Ring middleware - sync

(defn- make-partitioned-limiter
  [total-limit partitions]
  (flux/partitioned-limiter
   (flux/simple-limiter (flux/fixed-limit {:limit total-limit}))
   identity
   partitions))

(deftest partitioned-limiter-test
  (testing "admits request within partition budget"
    (let [lim (make-partitioned-limiter 10 {:live 0.8 :batch 0.2})
          l (flux/acquire! lim :live)]
      (is (some? l))
      (flux/success! l)))

  (testing "rejects when partition budget is exhausted"
    ;; live budget = floor(10 * 0.8) = 8
    (let [lim (make-partitioned-limiter 10 {:live 0.8 :batch 0.2})
          held (doall (repeatedly 8 #(flux/acquire! lim :live)))]
      (is (every? some? held))
      (is (nil? (flux/acquire! lim :live)))
      (doseq [l held] (flux/success! l))))

  (testing "partitions are independent — one exhausted does not block another"
    (let [lim (make-partitioned-limiter 10 {:live 0.5 :batch 0.5})
          live-slots (doall (repeatedly 5 #(flux/acquire! lim :live)))]
      (is (every? some? live-slots))
      ;; batch still has its own budget
      (let [batch-slot (flux/acquire! lim :batch)]
        (is (some? batch-slot))
        (flux/success! batch-slot))
      (doseq [l live-slots] (flux/success! l))))

  (testing "slot is released after success!"
    (let [lim (make-partitioned-limiter 10 {:live 0.5})
          l1 (flux/acquire! lim :live)]
      (flux/success! l1)
      (let [l2 (flux/acquire! lim :live)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "slot is released after ignore!"
    (let [lim (make-partitioned-limiter 10 {:live 0.5})
          l1 (flux/acquire! lim :live)]
      (flux/ignore! l1)
      (let [l2 (flux/acquire! lim :live)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "slot is released after dropped!"
    (let [lim (make-partitioned-limiter 10 {:live 0.5})
          l1 (flux/acquire! lim :live)]
      (flux/dropped! l1)
      (let [l2 (flux/acquire! lim :live)]
        (is (some? l2))
        (flux/success! l2))))

  (testing "nil partition key uses overflow capacity"
    ;; total=10, live=0.7 (budget=7), batch=0.2 (budget=2), overflow=1
    (let [lim (make-partitioned-limiter 10 {:live 0.7 :batch 0.2})
          l (flux/acquire! lim nil)]
      (is (some? l))
      (flux/success! l)))

  (testing "unknown partition key uses overflow capacity"
    (let [lim (make-partitioned-limiter 10 {:live 0.8})
          l (flux/acquire! lim :unknown)]
      (is (some? l))
      (flux/success! l)))

  (testing "overflow is blocked when partitions consume all capacity"
    ;; total=10, live=0.6 (6), batch=0.4 (4) — overflow=0
    (let [lim (make-partitioned-limiter 10 {:live 0.6 :batch 0.4})
          live-held (doall (repeatedly 6 #(flux/acquire! lim :live)))
          batch-held (doall (repeatedly 4 #(flux/acquire! lim :batch)))]
      (is (every? some? live-held))
      (is (every? some? batch-held))
      (is (nil? (flux/acquire! lim nil)))
      (doseq [l (concat live-held batch-held)] (flux/success! l)))))

(deftest ring-middleware-basic-test
  (testing "passes request through and returns response"
    (let [app (flux.ring/wrap-concurrency-limit
               (fn [_req] {:status 200 :body "ok"})
               (make-limiter 10))]
      (is (= {:status 200 :body "ok"} (app {}))))))

(deftest ring-middleware-reject-test
  (testing "returns 503 by default when limit exceeded"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit (fn [_] {:status 200}) limiter)]
          (is (= 503 (:status (app {}))))))))

  (testing "503 reject response includes Retry-After header"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit (fn [_] {:status 200}) limiter)
              resp (app {})]
          (is (get-in resp [:headers "Retry-After"]))))))

  (testing "custom on-reject fn is called with the request"
    (let [limiter (make-limiter 1)
          seen-req (atom nil)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit
                   (fn [_] {:status 200}) limiter
                   {:on-reject (fn [req] (reset! seen-req req) {:status 429})})]
          (app {:uri "/test"})
          (is (= "/test" (:uri @seen-req)))))))

  (testing "custom on-reject controls response status"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit
                   (fn [_] {:status 200}) limiter
                   {:on-reject (constantly {:status 429 :body "slow down"})})]
          (is (= 429 (:status (app {})))))))))

(deftest ring-middleware-classify-test
  (testing "5xx response (non-503) passes through and releases slot"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 500}) limiter)]
      (is (= 500 (:status (app {}))))
      ;; slot released - next request must be served
      (is (= 500 (:status (app {}))))))

  (testing "503 from app is treated as :success (not :dropped)"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 503}) limiter)]
      (is (= 503 (:status (app {}))))
      (is (= 503 (:status (app {}))))))

  (testing "2xx response releases slot"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 200}) limiter)]
      (app {})
      (is (some? (flux/acquire! limiter nil)))))

  (testing "custom classify fn receives the response"
    (let [limiter (make-limiter 10)
          classified (atom nil)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 201 :body "created"}) limiter
               {:classify (fn [resp]
                            (reset! classified (:status resp))
                            :success)})]
      (app {})
      (is (= 201 @classified))))

  (testing "custom classify :ignore releases slot"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 400}) limiter
               {:classify (constantly :ignore)})]
      (app {})
      (is (some? (flux/acquire! limiter nil)))))

  (testing "custom classify :dropped releases slot"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 500}) limiter
               {:classify (constantly :dropped)})]
      (app {})
      (is (some? (flux/acquire! limiter nil))))))

(deftest ring-middleware-exception-test
  (testing "exception propagates and slot is released"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] (throw (ex-info "handler error" {})))
               limiter)]
      (is (thrown? clojure.lang.ExceptionInfo (app {})))
      (is (some? (flux/acquire! limiter nil)))))

  (testing "on-error fn is called with request and throwable"
    (let [limiter (make-limiter 5)
          seen (atom nil)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] (throw (ex-info "oops" {:code 42})))
               limiter
               {:on-error (fn [req t]
                            (reset! seen {:req req :t t})
                            nil)})]
      (is (thrown? clojure.lang.ExceptionInfo (app {:uri "/boom"})))
      (is (= "/boom" (get-in @seen [:req :uri])))
      (is (= 42 (get-in @seen [:t (ex-data (:t @seen)) :code]
                        (-> @seen :t ex-data :code))))))

  (testing "on-error returning a response suppresses the exception"
    (let [limiter (make-limiter 5)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] (throw (RuntimeException. "boom")))
               limiter
               {:on-error (fn [_req _t] {:status 500 :body "handled"})})]
      (is (= {:status 500 :body "handled"} (app {})))))

  (testing "on-error returning nil still rethrows"
    (let [limiter (make-limiter 5)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] (throw (RuntimeException. "boom")))
               limiter
               {:on-error (fn [_req _t] nil)})]
      (is (thrown? RuntimeException (app {}))))))

(deftest ring-middleware-context-fn-test
  (testing "context-fn receives the full request map"
    (let [limiter (make-limiter 10)
          seen-ctx (atom nil)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 200}) limiter
               {:context-fn (fn [req] (reset! seen-ctx req) req)})]
      (app {:uri "/foo" :request-method :get})
      (is (= {:uri "/foo" :request-method :get} @seen-ctx))))

  (testing "context-fn return value is passed to the limiter as context"
    ;; We can verify this indirectly: if context-fn returns nil the limiter
    ;; still accepts it (SimpleLimiter ignores context value)
    (let [limiter (make-limiter 5)
          app (flux.ring/wrap-concurrency-limit
               (fn [_] {:status 200}) limiter
               {:context-fn (constantly nil)})]
      (is (= 200 (:status (app {})))))))

;;; Ring middleware - async

(deftest ring-middleware-async-test
  (testing "async arity passes response through respond"
    (let [limiter (make-limiter 10)
          app (flux.ring/wrap-concurrency-limit
               (fn [_req respond _raise] (respond {:status 200 :body "async"}))
               limiter)
          responses (atom [])]
      (app {} #(swap! responses conj %) #(throw %))
      (is (= [{:status 200 :body "async"}] @responses))))

  (testing "async arity calls respond with 503 when limit exceeded"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit
                   (fn [_req respond _raise] (respond {:status 200}))
                   limiter)
              responses (atom [])]
          (app {} #(swap! responses conj %) #(throw %))
          (is (= [503] (map :status @responses)))))))

  (testing "async arity calls respond with custom reject response"
    (let [limiter (make-limiter 1)]
      (with-acquired [_ limiter]
        (let [app (flux.ring/wrap-concurrency-limit
                   (fn [_req respond _raise] (respond {:status 200}))
                   limiter
                   {:on-reject (constantly {:status 429})})
              responses (atom [])]
          (app {} #(swap! responses conj %) #(throw %))
          (is (= [429] (map :status @responses)))))))

  (testing "async arity calls raise on exception"
    (let [limiter (make-limiter 10)
          app (flux.ring/wrap-concurrency-limit
               (fn [_req _respond raise] (raise (ex-info "async boom" {})))
               limiter)
          errors (atom [])]
      (app {} identity #(swap! errors conj %))
      (is (= 1 (count @errors)))
      (is (= "async boom" (ex-message (first @errors))))))

  (testing "async arity releases slot on exception"
    (let [limiter (make-limiter 1)
          app (flux.ring/wrap-concurrency-limit
               (fn [_req _respond raise] (raise (RuntimeException. "oops")))
               limiter)]
      (app {} identity identity)
      (is (some? (flux/acquire! limiter nil)))))

  (testing "async arity on-error returning response sends it via respond"
    (let [limiter (make-limiter 5)
          app (flux.ring/wrap-concurrency-limit
               (fn [_req _respond raise] (raise (RuntimeException. "boom")))
               limiter
               {:on-error (fn [_req _t] {:status 500 :body "handled"})})
          responses (atom [])]
      (app {} #(swap! responses conj %) #(throw %))
      (is (= [{:status 500 :body "handled"}] @responses))))

  (testing "async arity on-error returning nil calls raise"
    (let [limiter (make-limiter 5)
          app (flux.ring/wrap-concurrency-limit
               (fn [_req _respond raise] (raise (RuntimeException. "boom")))
               limiter
               {:on-error (fn [_req _t] nil)})
          errors (atom [])]
      (app {} identity #(swap! errors conj %))
      (is (= 1 (count @errors))))))
