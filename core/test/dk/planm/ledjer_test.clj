(ns dk.planm.ledjer-test
  (:require
   [clojure.test :as test :refer [deftest testing is]]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.event :as e]
   [dk.planm.ledjer.protocols :as p]))

(l/defevent counted
  :type :ledjer-test/counted
  :schema [:map [:id :string] [:num :int]]
  :tags [(e/payload-path :id)])

(l/defevent ignored
  :type :ledjer-test/ignored
  :schema [:map [:id :string]]
  :tags [(e/payload-path :id)])

(defn- report-types
  "Run f, returning the types of the clojure.test reports it made."
  [f]
  (let [reports (atom [])]
    (binding [test/report (fn [m] (swap! reports conj (:type m)))]
      (f))
    @reports))

(l/defevent reacted
  :type :ledjer-test/reacted
  :schema [:map [:id :string]]
  :tags [(e/payload-path :id)])

(defn- react-immediately
  "An automation appending a reacted event as soon as a counted event
  arrives."
  [event-store]
  (l/automation event-store
                #{(counted)}
                (fn [event-store]
                  (l/append event-store [(reacted {:id "a"})]))))

(deftest gt-automation
  (testing "the reaction of an automation"
    (is (= [:pass]
           (report-types
            #(l/gt :automation react-immediately
                   :given [(counted {:id "a" :num 1})]
                   :then [(reacted {:id "a"})])))))

  (testing "a reaction appended before gt subscribes for then is seen"
    ;; Delaying subscribe gives the automation time to react between
    ;; the append of given and gt subscribing for then. The automation
    ;; subscribes before given is appended, so the delay doesn't affect
    ;; it. A subscription for then from the current position would
    ;; miss the reaction.
    (let [subscribe l/subscribe]
      (with-redefs [l/subscribe (fn [& args]
                                  (Thread/sleep 100)
                                  (apply subscribe args))]
        (is (= [:pass]
               (report-types
                #(l/gt :automation react-immediately
                       :given [(counted {:id "a" :num 1})]
                       :then [(reacted {:id "a"})]
                       :timeout-ms 500)))))))

  (testing "no given events"
    (is (= []
           (report-types
            #(l/gt :automation react-immediately
                   :given []
                   :then []))))))

(defn- totals-acc
  "A PersistedAccumulator of totals per id, with a query fn, total.
  Ops are applied to the totals only on commit."
  []
  (let [state (atom {:totals {} :sequence nil})
        ops (atom [])]
    (reify
      p/PersistedAccumulator
      (-sequence-number [_] (:sequence @state))
      (-tx-append [this op] (swap! ops conj op) this)
      (-tx-commit [_ sequence-number]
        (swap! state (fn [s]
                       (-> (reduce (fn [s [id n]] (update-in s [:totals id] (fnil + 0) n))
                                   s
                                   @ops)
                           (assoc :sequence sequence-number))))
        (reset! ops []))
      (-reset [this] this)

      clojure.lang.IFn
      (invoke [_ id] (get-in @state [:totals id] 0)))))

(defn- total
  [acc id]
  (acc id))

(l/defpersistedview totals-view
  (counted)
  (fn [acc event]
    (let [{:keys [id num]} (e/payload event)]
      (p/-tx-append acc [id num]))))

(l/defpersistedview failing-view
  (counted)
  (fn [_ _] (throw (ex-info "Boom" {}))))

(deftest gt-persisted-view
  (testing "passes when the accumulator satisfies then"
    (is (= [:pass]
           (report-types
            #(l/gt :view #'totals-view
                   :acc (totals-acc)
                   :given [(counted {:id "a" :num 1})
                           (counted {:id "b" :num 5})
                           (counted {:id "a" :num 2})]
                   :then (fn [acc] (and (= 3 (total acc "a"))
                                        (= 5 (total acc "b")))))))))

  (testing "fails when the accumulator does not satisfy then"
    (is (= [:fail]
           (report-types
            #(l/gt :view #'totals-view
                   :acc (totals-acc)
                   :given [(counted {:id "a" :num 1})]
                   :then (fn [acc] (= 2 (total acc "a"))))))))

  (testing "waits for the last applied event, when the last given event isn't applied"
    (is (= [:pass]
           (report-types
            #(l/gt :view #'totals-view
                   :acc (totals-acc)
                   :given [(counted {:id "a" :num 1})
                           (ignored {:id "a"})]
                   :then (fn [acc] (= 1 (total acc "a"))))))))

  (testing "no given events"
    (is (= [:pass]
           (report-types
            #(l/gt :view #'totals-view
                   :acc (totals-acc)
                   :given []
                   :then (fn [acc] (= 0 (total acc "a"))))))))

  (testing "a view failure is thrown"
    (let [ex (try
               (l/gt :view #'failing-view
                     :acc (totals-acc)
                     :given [(counted {:id "a" :num 1})]
                     :then (constantly true))
               nil
               (catch clojure.lang.ExceptionInfo ex ex))]
      (is (= :reduce (:phase (ex-data ex))))))

  (testing "a used accumulator is rejected"
    (let [acc (totals-acc)]
      (p/-tx-commit acc 1)
      (is (thrown-with-msg? AssertionError #"must be fresh"
                            (l/gt :view #'totals-view
                                  :acc acc
                                  :given []
                                  :then (constantly true))))))

  (testing "exactly one of :automation and :view"
    (is (thrown-with-msg? AssertionError #"exactly one"
                          (l/gt :given [] :then [])))
    (is (thrown-with-msg? AssertionError #"exactly one"
                          (l/gt :automation (fn [_] (fn []))
                                :view #'totals-view
                                :given []
                                :then [])))))
