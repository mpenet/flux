(ns s-exp.flux.ring
  "Ring middleware for concurrency limiting.

  Usage:

    (require '[s-exp.flux :as flux]
             '[s-exp.flux.ring :as flux.ring])

    (def limiter
      (flux/simple-limiter (flux/vegas-limit {:max-concurrency 200})))

    (def app
      (-> handler
          (flux.ring/wrap-concurrency-limit limiter)))

  By default:
  - The context passed to the limiter is the Ring request map.
  - A 503 response is returned when the limit is exceeded.
  - 5xx responses (except 503 from the app itself) signal a drop.
  - All other responses signal success.

  These behaviours are customisable via options."
  (:require [s-exp.flux :as flux]))

(def ^:private default-reject-response
  {:status 503
   :headers {"Content-Type" "text/plain" "Retry-After" "1"}
   :body "Service temporarily unavailable - concurrency limit exceeded"})

(defn- default-classify
  "Maps a Ring response to a limiter outcome keyword.
  - 5xx (non-503)  → :dropped  (signals degradation to the algorithm)
  - everything else → :success"
  [response]
  (let [status (:status response)]
    (if (and (>= status 500) (not= status 503))
      :dropped
      :success)))

(defn wrap-concurrency-limit
  "Ring middleware that enforces a concurrency limit.

  `limiter` - a `SimpleLimiter` (or any `com.netflix.concurrency.limits.Limiter`)

  Options:
    :context-fn     (fn [request] -> context)
                    Builds the limiter context from the Ring request.
                    Defaults to the request map itself.

    :on-reject      (fn [request] -> response)
                    Called when the limit is exceeded.
                    Defaults to a 503 response.

    :classify       (fn [response] -> :success | :ignore | :dropped)
                    Maps the Ring response to a limiter signal.
                    Defaults to treating 5xx as :dropped, rest as :success.

    :on-error       (fn [request throwable] -> response | nil)
                    Called when the handler throws. If it returns a response,
                    that response is returned to the caller (the throwable is
                    still rethrown if nil is returned). The limiter always
                    receives :dropped on exception.
                    Defaults to nil (exceptions propagate)."
  ([handler limiter]
   (wrap-concurrency-limit handler limiter {}))
  ([handler limiter {:keys [context-fn on-reject classify on-error]
                     :or {context-fn identity
                          on-reject (constantly default-reject-response)
                          classify default-classify}}]
   (fn
     ([request]
      (if-let [listener (flux/acquire! limiter (context-fn request))]
        (try
          (let [response (handler request)
                outcome (classify response)]
            (case outcome
              :success (flux/success! listener)
              :ignore (flux/ignore! listener)
              :dropped (flux/dropped! listener))
            response)
          (catch Throwable t
            (flux/dropped! listener)
            (if on-error
              (or (on-error request t) (throw t))
              (throw t))))
        (on-reject request)))
     ([request respond raise]
      (if-let [listener (flux/acquire! limiter (context-fn request))]
        (handler request
                 (fn [response]
                   (let [outcome (classify response)]
                     (case outcome
                       :success (flux/success! listener)
                       :ignore (flux/ignore! listener)
                       :dropped (flux/dropped! listener)))
                   (respond response))
                 (fn [t]
                   (flux/dropped! listener)
                   (if on-error
                     (if-let [response (on-error request t)]
                       (respond response)
                       (raise t))
                     (raise t))))
        (respond (on-reject request)))))))
