(ns dk.planm.ledjer.event-test
  (:require
   [clojure.test :as test :refer [deftest is]]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.event :as e]))

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

(let [payload {:id "test-123"
               :num 42}
      metadata {:happy true}
      event (test-event metadata payload)]
  (deftest payload
    (is (= payload (e/payload event))))

  (deftest metadata
    (is (= (assoc metadata :version 1) (e/metadata event))))

  (deftest payload-path
    (is (= [:event/payload :id] (e/payload-path :id))))

  (deftest metadata-path
    (is (= [:event/metadata :version] (e/metadata-path :version))))

  (deftest get-event-tags
    (is (= [(e/payload-path :id)] (e/get-event-tags (test-event)))))

  (deftest event-type
    (is (= (test-event) (e/event-type event))))

  (deftest event->tag-bindings
    (is (= {(e/payload-path :id) "test-123"} (e/event->tag-bindings event))))

  (deftest has-bindings?
    (is (= true (e/has-bindings? event {(e/payload-path :id) "test-123"})))
    (is (= true (e/has-bindings? event {(e/payload-path :id) "*"})))
    (is (= true (e/has-bindings? event {(e/payload-path :num) 42})))
    (is (= false (e/has-bindings? event {(e/payload-path :num) 41})))
    (is (= false (e/has-bindings? event {(e/payload-path :id) "foobar"})))
    (let [event-2 (test-event-2 payload)]
      (is (= true (e/has-bindings? event-2 {(e/payload-path :id) "test-123"
                                            (e/metadata-path :version) 1}))))))
