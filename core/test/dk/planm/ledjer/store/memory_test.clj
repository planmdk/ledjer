(ns dk.planm.ledjer.store.memory-test
  (:require
   [clojure.core.async :as async]
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

;; Subscriptions

(l/defevent order-placed
  :type :memory-test/order-placed
  :schema [:map [:order-id :uuid]]
  :tags [(e/payload-path :order-id)])

;; Has an :order-id too, but doesn't tag it
(l/defevent order-noted
  :type :memory-test/order-noted
  :schema [:map [:order-id :uuid] [:note :string]]
  :tags [])

(defn- receive-all
  "Events arriving on c until none arrive for 200 ms."
  [c]
  (loop [received []]
    (let [v (async/alt!! c ([v] v) (async/timeout 200) ::timeout)]
      (if (= ::timeout v)
        received
        (recur (conj received v))))))

(defn- order-ids
  [events]
  (mapv (comp :order-id e/payload) events))

(defn- watch-count
  [event-store]
  (count (.getWatches ^clojure.lang.IRef (:last-sequence-number event-store))))

(deftest subscribe
  (testing "every subscription receives events, not just the latest"
    (let [event-store (store.memory/in-memory-event-store)
          query #{{:event-types #{(order-placed)}}}
          c-1 (l/subscribe event-store query)
          c-2 (l/subscribe event-store query)
          id (random-uuid)]
      (l/append event-store [(order-placed {:order-id id})])
      (is (= [id] (order-ids (receive-all c-1))))
      (is (= [id] (order-ids (receive-all c-2))))))

  (testing "all events from one append are delivered, in order"
    (let [event-store (store.memory/in-memory-event-store)
          c (l/subscribe event-store #{{:event-types #{(order-placed)}}})
          ids (repeatedly 3 random-uuid)]
      (l/append event-store (mapv #(order-placed {:order-id %}) ids))
      (is (= (vec ids) (order-ids (receive-all c))))))

  (testing "only events appended after subscribing are delivered"
    (let [event-store (store.memory/in-memory-event-store)
          before (random-uuid)
          after (random-uuid)]
      (l/append event-store [(order-placed {:order-id before})])
      (let [c (l/subscribe event-store #{{:event-types #{(order-placed)}}})]
        (l/append event-store [(order-placed {:order-id after})])
        (is (= [after] (order-ids (receive-all c)))))))

  (testing "an event matching several query maps is delivered once"
    (let [event-store (store.memory/in-memory-event-store)
          id (random-uuid)
          c (l/subscribe event-store #{{:event-types #{(order-placed)}}
                                       {:event-types #{(order-placed)}
                                        :tag-bindings {(e/payload-path :order-id) id}}})]
      (l/append event-store [(order-placed {:order-id id})])
      (is (= [id] (order-ids (receive-all c))))))

  (testing "tag bindings select events like source does"
    (let [event-store (store.memory/in-memory-event-store)
          wanted (random-uuid)
          other (random-uuid)
          query #{{:event-types #{(order-placed) (order-noted)}
                   :tag-bindings {(e/payload-path :order-id) wanted}}}
          c (l/subscribe event-store query)]
      (l/append event-store [(order-placed {:order-id wanted})
                             (order-placed {:order-id other})
                             ;; order-noted doesn't tag :order-id, so the
                             ;; binding doesn't apply to it
                             (order-noted {:order-id other :note "x"})])
      (is (= (order-ids (l/source event-store query))
             (order-ids (receive-all c))
             [wanted other]))))

  (testing "closing the channel ends the subscription"
    (let [event-store (store.memory/in-memory-event-store)
          c (l/subscribe event-store #{{:event-types #{(order-placed)}}})]
      (is (= 1 (watch-count event-store)))
      (async/close! c)
      (l/append event-store [(order-placed {:order-id (random-uuid)})])
      (is (= 0 (watch-count event-store))))))
