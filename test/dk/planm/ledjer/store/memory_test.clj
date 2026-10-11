(ns dk.planm.ledjer.store.memory-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.event :as e]
   [dk.planm.ledjer.store.memory :as store.memory]))

(l/defevent uuid-tagged-event
  :type :test/uuid-tagged-event
  :schema [:map
           [:id :uuid]
           [:num :int]]
  :tags [(e/payload-path :id)])

(deftest source-with-uuid-tags
  (let [event-store (store.memory/in-memory-event-store)
        id-1 (random-uuid)
        id-2 (random-uuid)]
    (l/append event-store [(uuid-tagged-event {:id id-1 :num 1})
                           (uuid-tagged-event {:id id-2 :num 2})])
    (testing "bound to a UUID"
      (is (= [1]
             (map (comp :num e/payload)
                  (l/source event-store #{{:event-types #{(uuid-tagged-event)}
                                           :tag-bindings {(e/payload-path :id) id-1}}})))))
    (testing "unbound"
      (is (= [1 2]
             (map (comp :num e/payload)
                  (l/source event-store #{{:event-types #{(uuid-tagged-event)}
                                           :tag-bindings {}}})))))
    (testing "\"*\" matches any value"
      (is (= [1 2]
             (map (comp :num e/payload)
                  (l/source event-store #{{:event-types #{(uuid-tagged-event)}
                                           :tag-bindings {(e/payload-path :id) "*"}}})))))
    (testing "values match on type as well as value"
      (is (empty? (l/source event-store #{{:event-types #{(uuid-tagged-event)}
                                           :tag-bindings {(e/payload-path :id) (str id-1)}}}))))))
