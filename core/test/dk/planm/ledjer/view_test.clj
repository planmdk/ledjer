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
      (is (= 10 (v/view* event-store binding-pairs {(e/payload-path :id) "abc-123"} initial-acc)))))
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
  "A PersistedAccumulator recording what was committed.

  commit-fn is called first in -tx-commit with the sequence number. It
  may throw to simulate a failing commit, which leaves the open
  transaction intact."
  [& {:keys [commit-fn] :or {commit-fn (fn [_])}}]
  (let [ops (atom [])
        commits (atom [])]
    {:ops ops
     :commits commits
     :acc (reify p/PersistedAccumulator
            (-sequence-number [_] (:sequence (peek @commits)))
            (-tx-append [this op] (swap! ops conj op) this)
            (-tx-commit [_ sequence-number]
              (commit-fn sequence-number)
              (swap! commits conj {:sequence sequence-number :ops @ops})
              (reset! ops []))
            (-reset [this] this))}))

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
