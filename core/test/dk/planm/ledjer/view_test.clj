(ns dk.planm.ledjer.view-test
  (:require
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.view :as v]
   [dk.planm.ledjer.event :as e]
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
