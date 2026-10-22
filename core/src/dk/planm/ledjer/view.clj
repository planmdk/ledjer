(ns dk.planm.ledjer.view
  (:require
   [clojure.core.async :as async]
   [dk.planm.ledjer.event :as event]
   [dk.planm.ledjer.protocols :as p]
   [dk.planm.ledjer.query :as query]))

(defn- binding-pair-type
  "Get the event type from a state view binding pair."
  [binding-pair]
  (let [[kw-or-vec _] binding-pair]
    (if (keyword? kw-or-vec)
      kw-or-vec
      (first kw-or-vec))))

(defn- binding-pair-tags
  "Get the tag bindings from a state view binding pair."
  [binding-pair]
  (let [[kw-or-vec _] binding-pair]
    (if (keyword? kw-or-vec)
      {}
      (second kw-or-vec))))

(defn- binding-pair-reducer
  "Get the event reducer from a state view binding pair."
  [binding-pair]
  (let [[_ reducer] binding-pair]
    reducer))

(defn- binding-pair-reducer-for
  "Get the event reducer for event from the state view binding-pairs."
  [binding-pairs event]
  (let [pairs-matching-type (into {}
                                  (comp
                                   (filter (fn type-filter [pair]
                                             (= (binding-pair-type pair) (event/event-type event))))
                                   (filter (fn binding-filter [pair]
                                             (event/has-bindings? event (binding-pair-tags pair)))))
                                  binding-pairs)]
    (if (not= 1 (count pairs-matching-type))
      (throw (ex-info "Expected exactly one matching binding pair per event!" {:num-matching (count pairs-matching-type)}))
      (binding-pair-reducer (first pairs-matching-type)))))

(defn- binding-pair-query-tags
  "The tag bindings to query binding-pair's event type with: the
  binding pair's own tags, plus those of the runtime tag-bindings that
  name one of the event type's tags.

  A \"*\" on either side yields to the other. Otherwise, a runtime
  binding differing from the binding pair's is a conflict and throws."
  [binding-pair tag-bindings]
  (let [pair-tags (binding-pair-tags binding-pair)
        runtime-tags (select-keys tag-bindings
                                  (event/get-event-tags (binding-pair-type binding-pair)))
        conflicts (into {}
                        (filter (fn [[tag v]]
                                  (and (contains? pair-tags tag)
                                       (let [pv (get pair-tags tag)]
                                         (not (or (= pv v) (= "*" pv) (= "*" v)))))))
                        runtime-tags)]
    (when (seq conflicts)
      (throw (ex-info "Tag bindings conflict with binding pair!"
                      {:event-type (binding-pair-type binding-pair)
                       :binding-pair-tags pair-tags
                       :conflicting-tag-bindings conflicts})))
    (merge-with (fn [runtime-v pair-v] (if (= "*" pair-v) runtime-v pair-v))
                runtime-tags
                pair-tags)))

(defn- validate-tag-bindings!
  "Throw if tag-bindings names a tag that none of binding-pairs' event
  types have. Such a binding applies to nothing, so is a mistake."
  [binding-pairs tag-bindings]
  (let [known (into #{}
                    (comp (map binding-pair-type)
                          (mapcat event/get-event-tags))
                    binding-pairs)
        unknown (into [] (remove known) (keys tag-bindings))]
    (when (seq unknown)
      (throw (ex-info "Invalid ledjer query: tag binding for a tag none of the event types have"
                      {:event-types (into #{} (map binding-pair-type) binding-pairs)
                       :unknown-tag-paths unknown})))))

(defn view*
  "Given a map of binding-pairs, either kw -> reducer-fn or [kw
  tag-bindings] -> reducer-fn, and a map of tag-bindings, reduces
  events sourced from event-store to a single value using the
  reducer-fns.

  The tag-bindings narrow each binding pair's query, for the tags the
  pair's event type has. They must name a tag of at least one of the
  binding pairs' event types, and must not conflict with the tags bound
  by a binding pair, or this throws.

  Will throw in case an event is sourced for which there is no
  reducer-fn registered."
  [event-store binding-pairs tag-bindings initial-acc]
  (validate-tag-bindings! binding-pairs tag-bindings)
  (let [events (p/-source event-store
                          (query/validate!
                           (into #{}
                                 (map (fn [binding-pair]
                                        {:event-types #{(binding-pair-type binding-pair)}
                                         :tag-bindings (binding-pair-query-tags binding-pair tag-bindings)}))
                                 binding-pairs))
                          {})]
    (reduce
     (fn [acc event]
       (let [event-reducer (binding-pair-reducer-for binding-pairs event)]
         (event-reducer acc event)))
     initial-acc
     events)))

(defn- apply-event!
  "Route event to its reducer and apply it to acc. Failures are
  rethrown as ex-info with `:phase :reduce`."
  [acc binding-pairs event]
  (try
    (let [reducer-fn (binding-pair-reducer-for binding-pairs event)]
      (reducer-fn acc event))
    (catch Throwable t
      (throw (ex-info "Persisted view reducer failed"
                      {:phase :reduce
                       :event/type (event/event-type event)
                       :sequence (:sequence (event/metadata event))}
                      t)))))

(defn- commit!
  "Commit the open transaction of acc along with sequence-number.

  A failed commit is retried up to `retries` times with exponential
  backoff starting at `backoff-ms`, relying on a failed `-tx-commit`
  leaving the open transaction intact. When retries are exhausted,
  throws ex-info with `:phase :commit`."
  [acc sequence-number retries backoff-ms]
  (loop [attempt 1
         backoff backoff-ms]
    (when-let [e (try
                   (p/-tx-commit acc sequence-number)
                   nil
                   (catch Exception e e))]
      (if (or (instance? InterruptedException e)
              (> attempt retries))
        (throw (ex-info "Persisted view commit failed"
                        {:phase :commit
                         :sequence sequence-number
                         :attempts attempt}
                        e))
        (do
          (Thread/sleep (long backoff))
          (recur (inc attempt) (* 2 backoff)))))))

(defn- subscribe-persisted-view
  "Subscribe to the events that binding-pairs apply to, starting at
  start-sequence. Returns the subscription channel. Failures are
  rethrown as ex-info with `:phase :subscribe`."
  [event-store binding-pairs start-sequence]
  (let [sub-query (query/validate!
                   (into #{}
                         (map (fn [binding-pair]
                                {:event-types #{(binding-pair-type binding-pair)}
                                 :tag-bindings (binding-pair-tags binding-pair)}))
                         binding-pairs))]
    (try
      (p/-subscribe event-store sub-query {:start-sequence start-sequence})
      (catch Throwable t
        (throw (ex-info "Persisted view subscription failed"
                        {:phase :subscribe
                         :start-sequence start-sequence}
                        t))))))

(defn- run-persisted-view!
  "On a new thread, apply the events arriving on sub-chan to acc and
  commit them in batches, until stop-chan is closed or a failure
  occurs. Takes ownership of sub-chan, closing it when done.

  Returns a channel that closes when the thread is done. On failure,
  it first resets failure to the exception and calls on-error, and
  the channel yields the exception."
  [acc
   binding-pairs
   sub-chan
   stop-chan
   failure
   {:keys [max-size max-ms commit-retries commit-backoff-ms on-error]
    :or {max-size 64
         max-ms 200
         commit-retries 3
         commit-backoff-ms 100
         on-error (fn rethrow [t] (throw t))}}]
  (let [never-chan (async/chan 1)]
    (async/thread
      (try
        (loop [n 0
               last-seq nil
               deadline nil]
          (let [timeout-chan (if deadline
                               (async/timeout (max 0 (- deadline (System/currentTimeMillis))))
                               never-chan)
                [event p] (async/alts!! [stop-chan timeout-chan sub-chan]
                                        :priority true)]
            (tap> [:persisted-view-sub-triggered {:stop? (= p stop-chan)}])
            (cond
              ;; Stopping flushes what has been applied
              (= p stop-chan) (when last-seq
                                (commit! acc last-seq commit-retries commit-backoff-ms))
              (= p timeout-chan) (do
                                   (commit! acc last-seq commit-retries commit-backoff-ms)
                                   (recur 0 nil nil))
              (nil? event) (throw (ex-info "Persisted view subscription closed unexpectedly"
                                           {:phase :subscribe
                                            :uncommitted-sequence last-seq}))
              :else (do
                      ;; reducer-fn is side-effecting on the acc
                      (apply-event! acc binding-pairs event)
                      (let [new-n (inc n)
                            seq-num (get (event/metadata event) :sequence)]
                        (if (>= new-n max-size)
                          (do
                            (commit! acc seq-num commit-retries commit-backoff-ms)
                            (recur 0 nil nil))
                          (recur new-n seq-num (or deadline (+ (System/currentTimeMillis) max-ms)))))))))
        (catch Throwable t
          (reset! failure t)
          (on-error t)
          t)
        (finally
          (async/close! sub-chan))))))

(defn persisted-view*
  "Keeps `acc` up to date with the events matching `binding-pairs`
  using a subscription on `event-store`, committing to `acc` every
  `max-size` events or `max-ms` milliseconds, whichever comes first.
  Returns a handle that derefs to `acc` and closes the view.

  Options:

  - max-size: events per commit (default 64)
  - max-ms: max time an event waits for commit (default 200)
  - commit-retries: retries of a failed commit (default 3)
  - commit-backoff-ms: delay before the first commit retry, doubled
  for each following retry (default 100)
  - on-error: fn of the failure, called on the worker thread after the
  view has stopped. The default rethrows, which ends up in the default
  uncaught exception handler.

  Failures are ex-info with `:phase` in ex-data:

  - :subscribe: subscribing failed, or the subscription closed
  unexpectedly. Subscribing failures at start are thrown by this
  function.
  - :reduce: no (or more than one) reducer for an event, or a reducer
  threw. Never skipped.
  - :commit: committing failed after retries.

  Once failed, deref rethrows the failure. Recover by closing the
  view and starting a new one: it resumes after the accumulator's last
  committed sequence number.

  Closing flushes the uncommitted events, then closes the accumulator
  if it is an AutoCloseable. Closing more than once is a no-op."
  [event-store binding-pairs acc & opts]
  (when-not (p/persisted-accumulator? acc)
    (throw (ex-info "acc must be a PersistedAccumulator" {})))
  (let [start-sequence (if-let [last-sequence-number (p/-sequence-number acc)]
                         (inc last-sequence-number)
                         0)
        sub-chan (subscribe-persisted-view event-store binding-pairs start-sequence)
        stop-chan (async/chan 1)
        failure (atom nil)
        closed? (atom false)
        worker (run-persisted-view! acc binding-pairs sub-chan stop-chan failure opts)]
    (reify
      clojure.lang.IDeref
      (deref [_]
        (when @failure
          (throw @failure))
        acc)

      java.lang.AutoCloseable
      (close [_]
        (when (compare-and-set! closed? false true)
          (async/close! stop-chan)
          (async/alt!!
            worker :done
            (async/timeout 5000) :timeout)
          (when (instance? java.lang.AutoCloseable acc)
            (.close acc)))))))
