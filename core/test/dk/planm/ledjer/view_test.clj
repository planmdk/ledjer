(ns dk.planm.ledjer.view-test
  (:require
   [clojure.core.async :as async]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.view :as v]
   [dk.planm.ledjer.event :as e]
   [dk.planm.ledjer.protocols :as p]
   [clojure.test :as test :refer [deftest testing is]]
   [dk.planm.ledjer.store.memory :as store.memory]))

(l/defevent test-event
  :type :test/event
  :schema [:map
           [:id :string]
           [:num :int]]
  :version 1
  :tags [(e/payload-path :id)])

(l/defevent test-event-2
  :type :test/event-2
  :schema [:map
           [:id :string]
           [:num :int]]
  :version 1
  :tags [(e/payload-path :id) (e/metadata-path :version)])

(deftest view*
  (testing "two events, one with tag-bindings specified"
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]

                         [[(test-event-2) {(e/payload-path :id) "abc-222"}]
                          (fn [acc ev]
                            (- acc (:num (e/payload ev))))]]
          tag-bindings {}
          initial-acc 0]
      (l/append event-store [(test-event {:id "abc-123" :num 10})
                             (test-event {:id "abc-222" :num 10})
                             (test-event-2 {:id "abc-222" :num 5})])
      (is (= 15 (v/view* event-store binding-pairs tag-bindings initial-acc)))
      (is (= 5 (v/view* event-store binding-pairs {(e/payload-path :id) "abc-222"} initial-acc)))
      (is (= 15 (v/view* event-store binding-pairs {(e/payload-path :id) "*"} initial-acc)))))
  (testing "tag-bindings conflicting with a binding pair's tag-bindings"
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]

                         [[(test-event-2) {(e/payload-path :id) "abc-222"}]
                          (fn [acc ev]
                            (- acc (:num (e/payload ev))))]]]
      (l/append event-store [(test-event {:id "abc-123" :num 10})
                             (test-event-2 {:id "abc-222" :num 5})])
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"conflict"
                            (v/view* event-store binding-pairs {(e/payload-path :id) "abc-123"} 0)))))
  (testing "tag-bindings narrow binding pairs without tag-bindings of their own"
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]

                         [(test-event-2)
                          (fn [acc ev]
                            (- acc (:num (e/payload ev))))]]]
      (l/append event-store [(test-event {:id "abc-123" :num 10})
                             (test-event {:id "abc-222" :num 10})
                             (test-event-2 {:id "abc-222" :num 5})])
      (is (= 5 (v/view* event-store binding-pairs {(e/payload-path :id) "abc-222"} 0)))))
  (testing "tag-bindings are only applied to event types having the tag"
    ;; Only test-event-2 has the metadata :version tag. Without
    ;; filtering, the query for test-event would be invalid.
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]

                         [(test-event-2)
                          (fn [acc ev]
                            (- acc (:num (e/payload ev))))]]]
      (l/append event-store [(test-event {:id "abc-123" :num 10})
                             (test-event {:id "abc-222" :num 10})
                             (test-event-2 {:id "abc-222" :num 5})])
      (is (= 15 (v/view* event-store binding-pairs {(e/metadata-path :version) 1} 0)))
      (is (= 20 (v/view* event-store binding-pairs {(e/metadata-path :version) 2} 0)))))
  (testing "tag-bindings for a tag none of the event types have"
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]]]
      (l/append event-store [(test-event {:id "abc-123" :num 10})])
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (v/view* event-store binding-pairs {(e/payload-path :nope) "x"} 0)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (v/view* event-store binding-pairs {(e/metadata-path :version) 1} 0)))))
  (testing "too many matching bindings"
    (let [event-store (store.memory/in-memory-event-store)
          binding-pairs [[(test-event)
                          (fn [acc ev]
                            (+ acc (:num (e/payload ev))))]

                         [[(test-event) {(e/payload-path :id) "abc-123"}]
                          (fn [acc ev]
                            (- acc (:num (e/payload ev))))]]
          tag-bindings {}
          initial-acc 0]
      (l/append event-store [(test-event {:id "abc-123" :num 10})])
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"exactly one matching binding pair"
                            (v/view* event-store binding-pairs tag-bindings initial-acc))))))

;; run-persisted-view! only depends on channels, so these tests feed
;; plain event maps through a plain channel into a stub accumulator.

(def ^:private run-view! #'v/run-persisted-view!)

(defn- stub-acc
  "A PersistedAccumulator recording what was committed, and the order
  of commits and close in :log.

  last-sequence is the sequence number of a previous run, if any.

  commit-fn is called first in -tx-commit with the sequence number. It
  may throw to simulate a failing commit, which leaves the open
  transaction intact."
  [& {:keys [commit-fn last-sequence] :or {commit-fn (fn [_])}}]
  (let [ops (atom [])
        commits (atom [])
        log (atom [])]
    {:ops ops
     :commits commits
     :log log
     :acc (reify
            p/PersistedAccumulator
            (-sequence-number [_] (or (:sequence (peek @commits)) last-sequence))
            (-tx-append [this op] (swap! ops conj op) this)
            (-tx-commit [_ sequence-number]
              (commit-fn sequence-number)
              (swap! commits conj {:sequence sequence-number :ops @ops})
              (swap! log conj [:commit sequence-number])
              (reset! ops []))
            (-reset [this] this)

            java.lang.AutoCloseable
            (close [_] (swap! log conj :close)))}))

(defn- ev
  [n]
  {:event/type :test/event
   :event/metadata {:sequence n}
   :event/payload {:num n}})

(def ^:private append-num-pairs
  [[:test/event (fn [acc event]
                  (p/-tx-append acc (:num (e/payload event)))
                  acc)]])

(defn- start!
  "Start the worker. Replaces the default on-error, which rethrows,
  with one recording the failures in :errors."
  [acc binding-pairs opts]
  (let [sub-chan (async/chan 100)
        stop-chan (async/chan 1)
        failure (atom nil)
        errors (atom [])
        worker (run-view! acc binding-pairs sub-chan stop-chan failure
                     (merge {:max-ms 60000
                             :on-error (fn [t] (swap! errors conj t))}
                            opts))]
    {:sub-chan sub-chan
     :stop-chan stop-chan
     :failure failure
     :errors errors
     :worker worker}))

(defn- await-worker
  "The value of the worker channel: the failure, or nil when it
  stopped cleanly."
  [worker]
  (async/alt!!
    worker ([v] v)
    (async/timeout 2000) ::timeout))

(defn- await-true
  [pred]
  (loop [tries 200]
    (cond
      (pred) true
      (zero? tries) false
      :else (do (Thread/sleep 10)
                (recur (dec tries))))))

(deftest run-persisted-view!-test
  (testing "commits every max-size events, and flushes the rest on stop"
    (let [{:keys [acc ops commits]} (stub-acc)
          {:keys [sub-chan stop-chan worker]} (start! acc append-num-pairs {:max-size 3})]
      (doseq [n (range 1 6)]
        (async/>!! sub-chan (ev n)))
      (is (await-true #(and (= [3] (mapv :sequence @commits))
                            (= [4 5] @ops))))
      (async/close! stop-chan)
      (is (nil? (await-worker worker)))
      (is (= [{:sequence 3 :ops [1 2 3]}
              {:sequence 5 :ops [4 5]}]
             @commits))))

  (testing "commits after max-ms, and does not commit again on stop"
    (let [{:keys [acc commits]} (stub-acc)
          {:keys [sub-chan stop-chan worker]} (start! acc append-num-pairs {:max-size 100
                                                                            :max-ms 100})]
      (async/>!! sub-chan (ev 1))
      (async/>!! sub-chan (ev 2))
      (is (await-true #(= 2 (:sequence (peek @commits)))))
      (let [num-commits (count @commits)]
        (async/close! stop-chan)
        (is (nil? (await-worker worker)))
        (is (= num-commits (count @commits))))
      (is (= [1 2] (into [] (mapcat :ops) @commits)))))

  (testing "stopping without events commits nothing"
    (let [{:keys [acc commits]} (stub-acc)
          {:keys [stop-chan worker]} (start! acc append-num-pairs {})]
      (async/close! stop-chan)
      (is (nil? (await-worker worker)))
      (is (empty? @commits))))

  (testing "closes the subscription channel when done"
    (let [{:keys [acc]} (stub-acc)
          {:keys [sub-chan stop-chan worker]} (start! acc append-num-pairs {})]
      (async/close! stop-chan)
      (await-worker worker)
      (is (false? (async/put! sub-chan (ev 1))))))

  (testing "reduce failure: no matching binding pair"
    (let [{:keys [acc commits]} (stub-acc)
          {:keys [sub-chan worker failure errors]} (start! acc append-num-pairs {})]
      (async/>!! sub-chan (ev 1))
      (async/>!! sub-chan (assoc (ev 2) :event/type :test/unknown))
      (let [ex (await-worker worker)]
        (is (= :reduce (:phase (ex-data ex))))
        (is (= :test/unknown (:event/type (ex-data ex))))
        (is (= 2 (:sequence (ex-data ex))))
        (is (re-find #"exactly one matching binding pair" (ex-message (ex-cause ex))))
        (is (identical? ex @failure))
        (is (= [ex] @errors)))
      (is (empty? @commits) "nothing is committed past a failed event")))

  (testing "reduce failure: reducer throws"
    (let [{:keys [acc]} (stub-acc)
          boom (ex-info "boom" {})
          {:keys [sub-chan worker]} (start! acc [[:test/event (fn [_ _] (throw boom))]] {})]
      (async/>!! sub-chan (ev 1))
      (let [ex (await-worker worker)]
        (is (= :reduce (:phase (ex-data ex))))
        (is (identical? boom (ex-cause ex))))))

  (testing "commit failure: retries are exhausted"
    (let [calls (atom 0)
          {:keys [acc commits]} (stub-acc :commit-fn (fn [_]
                                                       (swap! calls inc)
                                                       (throw (ex-info "index down" {}))))
          {:keys [sub-chan worker failure errors]} (start! acc append-num-pairs {:max-size 1
                                                                                 :commit-retries 2
                                                                                 :commit-backoff-ms 1})]
      (async/>!! sub-chan (ev 1))
      (let [ex (await-worker worker)]
        (is (= :commit (:phase (ex-data ex))))
        (is (= 1 (:sequence (ex-data ex))))
        (is (= 3 (:attempts (ex-data ex))))
        (is (= "index down" (ex-message (ex-cause ex))))
        (is (identical? ex @failure))
        (is (= [ex] @errors)))
      (is (= 3 @calls))
      (is (empty? @commits))))

  (testing "commit failure: a retry succeeds with the transaction intact"
    (let [calls (atom 0)
          {:keys [acc commits]} (stub-acc :commit-fn (fn [_]
                                                       (when (<= (swap! calls inc) 2)
                                                         (throw (ex-info "index down" {})))))
          {:keys [sub-chan stop-chan worker failure]} (start! acc append-num-pairs {:max-size 1
                                                                                    :commit-retries 3
                                                                                    :commit-backoff-ms 1})]
      (async/>!! sub-chan (ev 1))
      (is (await-true #(seq @commits)))
      (async/close! stop-chan)
      (is (nil? (await-worker worker)))
      (is (= 3 @calls))
      (is (= [{:sequence 1 :ops [1]}] @commits))
      (is (nil? @failure))))

  (testing "subscription failure: closed unexpectedly"
    (let [{:keys [acc]} (stub-acc)
          {:keys [sub-chan worker failure errors]} (start! acc append-num-pairs {})]
      (async/>!! sub-chan (ev 1))
      ;; Buffered events are delivered before the close is seen
      (async/close! sub-chan)
      (let [ex (await-worker worker)]
        (is (= :subscribe (:phase (ex-data ex))))
        (is (= 1 (:uncommitted-sequence (ex-data ex))))
        (is (re-find #"closed unexpectedly" (ex-message ex)))
        (is (identical? ex @failure))
        (is (= [ex] @errors))))))

;; persisted-view* is tested against a stub event store, which records
;; how it is subscribed to and hands out a channel for the test to feed.

(defn- stub-event-store
  "An event store that only supports -subscribe. The subscriptions
  made are recorded in :calls. It returns sub-chan, or throws
  subscribe-error if given."
  [sub-chan & {:keys [subscribe-error]}]
  (let [calls (atom [])]
    {:calls calls
     :event-store (with-meta {}
                    {`p/-subscribe (fn [_ query opts]
                                     (swap! calls conj {:query query :opts opts})
                                     (when subscribe-error
                                       (throw subscribe-error))
                                     sub-chan)})}))

(defn- close!
  [view]
  (.close ^java.lang.AutoCloseable view))

(defn- failed?
  [view]
  (try
    @view
    false
    (catch clojure.lang.ExceptionInfo _
      true)))

(deftest persisted-view*-test
  (testing "rejects an accumulator that is not a PersistedAccumulator"
    (let [{:keys [event-store]} (stub-event-store (async/chan))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"must be a PersistedAccumulator"
                            (v/persisted-view* event-store append-num-pairs {})))))

  (testing "subscribes from the start for a fresh accumulator"
    (let [{:keys [acc]} (stub-acc)
          {:keys [event-store calls]} (stub-event-store (async/chan))]
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc)]
        (is (= [{:start-sequence 0}] (map :opts @calls))))))

  (testing "resumes after the last sequence number of the accumulator"
    (let [{:keys [acc]} (stub-acc :last-sequence 41)
          {:keys [event-store calls]} (stub-event-store (async/chan))]
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc)]
        (is (= [{:start-sequence 42}] (map :opts @calls))))))

  (testing "subscribes with one query map per binding pair"
    (let [{:keys [acc]} (stub-acc)
          {:keys [event-store calls]} (stub-event-store (async/chan))
          binding-pairs [[(test-event) (fn [acc _] acc)]
                         [[(test-event-2) {(e/payload-path :id) "abc-222"}] (fn [acc _] acc)]]]
      (with-open [_ (v/persisted-view* event-store binding-pairs acc)]
        (is (= [#{{:event-types #{:test/event}
                   :tag-bindings {}}
                  {:event-types #{:test/event-2}
                   :tag-bindings {(e/payload-path :id) "abc-222"}}}]
               (map :query @calls))))))

  (testing "a subscribe failure at start is thrown, with the phase"
    (let [{:keys [acc]} (stub-acc :last-sequence 9)
          cause (ex-info "store down" {})
          {:keys [event-store]} (stub-event-store (async/chan) :subscribe-error cause)
          ex (try
               (v/persisted-view* event-store append-num-pairs acc)
               (catch clojure.lang.ExceptionInfo ex ex))]
      (is (= :subscribe (:phase (ex-data ex))))
      (is (= 10 (:start-sequence (ex-data ex))))
      (is (identical? cause (ex-cause ex)))))

  (testing "options are passed on to the worker"
    (let [{:keys [acc commits]} (stub-acc)
          sub-chan (async/chan 10)
          {:keys [event-store]} (stub-event-store sub-chan)]
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc
                                       :max-size 1
                                       :max-ms 60000)]
        (async/>!! sub-chan (ev 1))
        (is (await-true #(= [{:sequence 1 :ops [1]}] @commits))))))

  (testing "deref"
    (testing "returns the accumulator while healthy"
      (let [{:keys [acc]} (stub-acc)
            {:keys [event-store]} (stub-event-store (async/chan))]
        (with-open [view (v/persisted-view* event-store append-num-pairs acc)]
          (is (identical? acc @view)))))

    (testing "rethrows the failure once the view has failed, and keeps doing so"
      (let [{:keys [acc]} (stub-acc)
            sub-chan (async/chan 10)
            {:keys [event-store]} (stub-event-store sub-chan)]
        (with-open [view (v/persisted-view* event-store append-num-pairs acc
                                            :on-error (fn [_]))]
          (async/>!! sub-chan (assoc (ev 1) :event/type :test/unknown))
          (is (await-true #(failed? view)))
          (dotimes [_ 2]
            (let [ex (try @view (catch clojure.lang.ExceptionInfo ex ex))]
              (is (= :reduce (:phase (ex-data ex))))))))))

  (testing "close"
    (testing "flushes what was applied, then closes the accumulator and the subscription"
      (let [{:keys [acc ops log]} (stub-acc)
            sub-chan (async/chan 10)
            {:keys [event-store]} (stub-event-store sub-chan)
            view (v/persisted-view* event-store append-num-pairs acc :max-ms 60000)]
        (async/>!! sub-chan (ev 1))
        (is (await-true #(= [1] @ops)))
        (close! view)
        (is (= [[:commit 1] :close] @log))
        (is (false? (async/put! sub-chan (ev 2))))))

    (testing "is idempotent"
      (let [{:keys [acc log]} (stub-acc)
            {:keys [event-store]} (stub-event-store (async/chan))
            view (v/persisted-view* event-store append-num-pairs acc)]
        (close! view)
        (close! view)
        (is (= [:close] @log))))

    (testing "does not throw the failure, and still closes the accumulator"
      (let [{:keys [acc log]} (stub-acc)
            sub-chan (async/chan 10)
            {:keys [event-store]} (stub-event-store sub-chan)
            view (v/persisted-view* event-store append-num-pairs acc
                                    :on-error (fn [_]))]
        (async/>!! sub-chan (assoc (ev 1) :event/type :test/unknown))
        (is (await-true #(failed? view)))
        (close! view)
        (is (= [:close] @log))))))

;; persisted-view* with the in-memory event store. The accumulator
;; stands in for a persisted index: it outlives the views on it.

(defn- await-sequence
  "Wait for acc to commit up to and including sequence-number."
  [acc sequence-number]
  (await-true #(some-> (p/-sequence-number acc) (>= sequence-number))))

(defn- applied
  "The ops committed to a stub accumulator, in order."
  [commits]
  (into [] (mapcat :ops) @commits))

(deftest persisted-view*-in-memory-store
  (testing "applies existing events, then events as they arrive"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc commits]} (stub-acc)]
      (l/append event-store [(test-event {:id "a" :num 1})
                             (test-event {:id "b" :num 2})])
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc :max-ms 50)]
        (let [position (l/append event-store [(test-event {:id "c" :num 3})])]
          (is (await-sequence acc position))
          (is (= [1 2 3] (applied commits)))))))

  (testing "a restarted view resumes where the last one committed, applying every event once"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc commits]} (stub-acc)]
      (l/append event-store [(test-event {:id "a" :num 1})
                             (test-event {:id "b" :num 2})])
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc :max-ms 50)]
        (is (await-sequence acc 2)))
      ;; Appended while no view was running
      (l/append event-store [(test-event {:id "c" :num 3})
                             (test-event {:id "d" :num 4})])
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc :max-ms 50)]
        (let [position (l/append event-store [(test-event {:id "e" :num 5})])]
          (is (await-sequence acc position))))
      (is (= [1 2 3 4 5] (applied commits)))))

  (testing "a view applies just the events it has binding pairs for"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc commits]} (stub-acc)]
      (with-open [_ (v/persisted-view* event-store append-num-pairs acc :max-ms 50)]
        (let [position (l/append event-store [(test-event {:id "a" :num 1})
                                              (test-event-2 {:id "a" :num 2})
                                              (test-event {:id "a" :num 3})])]
          (is (await-sequence acc position))
          (is (= [1 3] (applied commits)))))))

  (testing "tag bindings in binding pairs select the events"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc commits]} (stub-acc)
          binding-pairs [[[(test-event) {(e/payload-path :id) "a"}]
                          (fn [acc event]
                            (p/-tx-append acc (:num (e/payload event)))
                            acc)]]]
      (l/append event-store [(test-event {:id "a" :num 1})
                             (test-event {:id "b" :num 2})])
      (with-open [_ (v/persisted-view* event-store binding-pairs acc :max-ms 50)]
        (let [position (l/append event-store [(test-event {:id "b" :num 3})
                                              (test-event {:id "a" :num 4})])]
          (is (await-sequence acc position))
          (is (= [1 4] (applied commits)))))))

  (testing "await-position waits for the commit of the last applied event"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc commits]} (stub-acc)]
      (with-open [view (v/persisted-view* event-store append-num-pairs acc :max-ms 50)]
        (let [position (l/append event-store [(test-event {:id "a" :num 1})
                                              (test-event-2 {:id "a" :num 2})])]
          (v/await-position view position)
          (is (= [1] (applied commits)))))))

  (testing "await-position times out when the view doesn't commit in time"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc]} (stub-acc)]
      (with-open [view (v/persisted-view* event-store append-num-pairs acc :max-ms 60000)]
        (let [position (l/append event-store [(test-event {:id "a" :num 1})])
              ex (try (v/await-position view position :timeout-ms 100)
                      nil
                      (catch clojure.lang.ExceptionInfo ex ex))]
          (is (= :await (:phase (ex-data ex))))
          (is (= 1 (:target-sequence (ex-data ex))))))))

  (testing "an event that several binding pairs match fails the view"
    (let [event-store (store.memory/in-memory-event-store)
          {:keys [acc]} (stub-acc)
          noop (fn [acc _] acc)
          binding-pairs [[(test-event) noop]
                         [[(test-event) {(e/payload-path :id) "a"}] noop]]]
      (with-open [view (v/persisted-view* event-store binding-pairs acc
                                          :on-error (fn [_]))]
        (l/append event-store [(test-event {:id "a" :num 1})])
        (is (await-true #(failed? view)))
        (let [ex (try @view (catch clojure.lang.ExceptionInfo ex ex))]
          (is (= :reduce (:phase (ex-data ex))))
          (is (re-find #"exactly one matching binding pair" (ex-message (ex-cause ex)))))))))
