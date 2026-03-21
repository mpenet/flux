(ns s-exp.flux-executor-test
  (:require [clojure.test :refer [deftest is testing]]
            [s-exp.flux :as flux]
            [s-exp.flux.executor :as flux.executor])
  (:import (java.util.concurrent RejectedExecutionException
                                 SynchronousQueue
                                 ThreadPoolExecutor
                                 TimeUnit)))

(deftest blocking-adaptive-executor-test
  (testing "creates executor with all defaults"
    (is (instance? com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor
                   (flux.executor/blocking-adaptive-executor {}))))

  (testing "creates executor with custom limiter"
    (let [limiter (flux/simple-limiter (flux/fixed-limit {:limit 5}))
          ex (flux.executor/blocking-adaptive-executor {:limiter limiter})]
      (is (instance? com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor ex))))

  (testing "creates executor with custom name"
    (is (instance? com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor
                   (flux.executor/blocking-adaptive-executor {:name "test-executor"}))))

  (testing "creates executor with custom underlying executor"
    (let [pool (java.util.concurrent.Executors/newCachedThreadPool)
          ex (flux.executor/blocking-adaptive-executor {:executor pool})]
      (is (instance? com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor ex))))

  (testing "execute! runs the fn"
    (let [ex (flux.executor/blocking-adaptive-executor {})
          result (promise)]
      (flux.executor/execute! ex #(deliver result :ran))
      (is (= :ran (deref result 1000 ::timeout)))))

  (testing "execute! runs multiple tasks up to concurrency limit"
    (let [ex (flux.executor/blocking-adaptive-executor
              {:limiter (flux/blocking-limiter
                         (flux/simple-limiter (flux/fixed-limit {:limit 3}))
                         {:timeout-ms 2000})})
          results (atom [])
          latch (java.util.concurrent.CountDownLatch. 3)]
      (dotimes [i 3]
        (flux.executor/execute! ex (fn []
                                     (swap! results conj i)
                                     (.countDown latch))))
      (.await latch 2 TimeUnit/SECONDS)
      (is (= 3 (count @results)))))

  (testing "execute! returns immediately after hand-off"
    (let [ex (flux.executor/blocking-adaptive-executor {})
          started (promise)
          blocking (promise)]
      (flux.executor/execute! ex (fn []
                                   (deliver started true)
                                   (deref blocking 2000 nil)))
      (is (deref started 1000 false))
      (deliver blocking :done))))

(deftest unchecked-timeout-exception-test
  (testing "no-arg constructor"
    (is (instance? com.netflix.concurrency.limits.executors.UncheckedTimeoutException
                   (flux.executor/unchecked-timeout-exception))))

  (testing "message constructor"
    (let [ex (flux.executor/unchecked-timeout-exception "timed out")]
      (is (instance? com.netflix.concurrency.limits.executors.UncheckedTimeoutException ex))
      (is (= "timed out" (.getMessage ex)))))

  (testing "message + cause constructor"
    (let [cause (RuntimeException. "root")
          ex (flux.executor/unchecked-timeout-exception "timed out" cause)]
      (is (= "timed out" (.getMessage ex)))
      (is (= cause (.getCause ex)))))

  (testing "thrown from a task signals dropped to the limiter"
    (let [limiter (flux/blocking-limiter
                   (flux/simple-limiter (flux/fixed-limit {:limit 5}))
                   {:timeout-ms 2000})
          ex (flux.executor/blocking-adaptive-executor {:limiter limiter})
          done (promise)]
      (flux.executor/execute! ex (fn []
                                   (try
                                     (throw (flux.executor/unchecked-timeout-exception "boom"))
                                     (finally (deliver done :done)))))
      (is (= :done (deref done 1000 ::timeout)))
      ;; slot must be released — further tasks should execute
      (let [result (promise)]
        (flux.executor/execute! ex #(deliver result :ok))
        (is (= :ok (deref result 1000 ::timeout)))))))
