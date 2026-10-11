(ns dk.planm.ledjer.query-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.event :as e]
   [dk.planm.ledjer.query :as query]
   [dk.planm.ledjer.store.memory :as store.memory]))

(l/defevent order-placed
  :type :query-test/order-placed
  :schema [:map [:order-id :uuid] [:customer-id :uuid]]
  :tags [(e/payload-path :order-id) (e/payload-path :customer-id)])

(l/defevent order-shipped
  :type :query-test/order-shipped
  :schema [:map [:order-id :uuid]]
  :tags [(e/payload-path :order-id)])

(l/defview order-count 0
  (order-placed)
  (fn [acc _] (inc acc)))

(defn- invalid? [q]
  (try (query/validate! q) false
       (catch clojure.lang.ExceptionInfo _ true)))

(deftest validate!
  (testing "valid queries"
    (is (not (invalid? #{{:event-types #{(order-placed)}}})))
    (is (not (invalid? #{{:event-types #{(order-placed)} :tag-bindings {}}})))
    (is (not (invalid? #{{:event-types #{(order-placed)} :tag-bindings nil}})))
    (is (not (invalid? #{{:event-types #{(order-placed)}
                          :tag-bindings {(e/payload-path :order-id) (random-uuid)}}})))
    (is (not (invalid? #{{:event-types #{(order-placed)}
                          :tag-bindings {(e/payload-path :order-id) "*"}}})))
    (testing "a binding needs only to be a tag of one of the event types"
      (is (not (invalid? #{{:event-types #{(order-placed) (order-shipped)}
                            :tag-bindings {(e/payload-path :customer-id) (random-uuid)}}})))))

  (testing "nil binding value"
    (is (invalid? #{{:event-types #{(order-placed)}
                     :tag-bindings {(e/payload-path :order-id) nil}}})))

  (testing "bare key instead of a tag path"
    (is (invalid? #{{:event-types #{(order-placed)}
                     :tag-bindings {:order-id (random-uuid)}}})))

  (testing "misspelled query map key"
    (is (invalid? #{{:event-types #{(order-placed)}
                     :tags-bindings {(e/payload-path :order-id) (random-uuid)}}})))

  (testing "tag path that none of the event types have"
    (is (invalid? #{{:event-types #{(order-shipped)}
                     :tag-bindings {(e/payload-path :customer-id) (random-uuid)}}})))

  (testing "event types instead of query maps"
    (is (invalid? #{(order-placed)}))))

(deftest queries-are-validated
  (let [event-store (store.memory/in-memory-event-store)
        bad-query #{{:event-types #{(order-placed)}
                     :tag-bindings {(e/payload-path :order-id) nil}}}]
    (testing "source"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (l/source event-store bad-query))))
    (testing "subscribe"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (l/subscribe event-store bad-query))))
    (testing "append condition"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (l/append event-store
                                      [(order-shipped {:order-id (random-uuid)})]
                                      {:query bad-query}))))
    (testing "views"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid ledjer query"
                            (order-count event-store {(e/payload-path :order-id) nil}))))))
