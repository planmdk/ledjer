(ns dk.planm.ledjer.store.umadb-test
  "Starts UmaDB in a container via Testcontainers, so a Docker-compatible
  runtime must be available. With Podman, start its socket and point
  Testcontainers at it:

      systemctl --user start podman.socket
      export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
      export TESTCONTAINERS_RYUK_DISABLED=true

  One container is shared by all tests in the namespace, so they only
  rely on events they append themselves."
  (:require
   [clj-test-containers.core :as tc]
   [clojure.core.async :as async]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [dk.planm.ledjer :as l]
   [dk.planm.ledjer.event :as e]
   [dk.planm.ledjer.store.umadb :as store.umadb])
  (:import
   [io.umadb.client UmaDbClient]))

(def ^:private umadb-port
  "UmaDB's port inside the container. Testcontainers maps it to a random
  free host port, so the tests don't clash with a local UmaDB."
  50051)

(def ^:dynamic *umadb-opts*
  "Connection options for the running UmaDB container."
  nil)

(def ^:dynamic *event-store* nil)

(use-fixtures :once
  (fn [f]
    (let [container (-> (tc/create {:image-name "docker.io/umadb/umadb:latest"
                                    :exposed-ports [umadb-port]
                                    :wait-for {:wait-strategy :port}})
                        tc/start!)]
      (try
        (binding [*umadb-opts* {:host (:host container)
                                :port (get (:mapped-ports container) umadb-port)}]
          (f))
        (finally
          (tc/stop! container))))))

(use-fixtures :each
  (fn [f]
    (let [client (store.umadb/umadb-event-store *umadb-opts*)]
      (.connect ^UmaDbClient client)
      (try
        (binding [*event-store* client]
          (f))
        (finally
          (store.umadb/close-umadb-client client))))))

(l/defevent order-placed
  :type :umadb-test/order-placed
  :schema [:map [:order-id :uuid]]
  :tags [(e/payload-path :order-id)])

(defn- receive-all
  "Events arriving on c until none arrive for 500 ms, or c is closed."
  [c]
  (loop [received []]
    (let [v (async/alt!! c ([v] v) (async/timeout 500) ::timeout)]
      (if (or (nil? v) (= ::timeout v))
        received
        (recur (conj received v))))))

(defn- order-ids
  [events]
  (mapv (comp :order-id e/payload) events))

(defn- orders-query
  "Only matches the orders with ids, so other tests' events are ignored."
  [ids]
  (into #{}
        (map (fn [id]
               {:event-types #{(order-placed)}
                :tag-bindings {(e/payload-path :order-id) id}}))
        ids))

(defn- append-orders!
  [ids]
  (l/append *event-store* (mapv #(order-placed {:order-id %}) ids)))

(deftest subscribe
  (testing "only events appended after subscribing are delivered"
    (let [[before after :as ids] (repeatedly 2 random-uuid)
          _ (append-orders! [before])
          c (l/subscribe *event-store* (orders-query ids))]
      (append-orders! [after])
      (is (= [after] (order-ids (receive-all c))))))

  (testing "every subscription receives events"
    (let [ids [(random-uuid)]
          c-1 (l/subscribe *event-store* (orders-query ids))
          c-2 (l/subscribe *event-store* (orders-query ids))]
      (append-orders! ids)
      (is (= ids (order-ids (receive-all c-1))))
      (is (= ids (order-ids (receive-all c-2))))))

  (testing "events carry their sequence number"
    (let [ids [(random-uuid)]
          c (l/subscribe *event-store* (orders-query ids))
          position (append-orders! ids)]
      (is (= [position]
             (map #(get-in % [:event/metadata :sequence]) (receive-all c)))))))

(deftest subscribe-with-start-sequence
  (testing "replays from the start sequence, inclusive, then continues live"
    (let [ids (vec (repeatedly 5 random-uuid))
          live-id (random-uuid)
          last-sequence (append-orders! ids)
          ;; The third of the five events
          c (l/subscribe *event-store* (orders-query (conj ids live-id))
                         {:start-sequence (- last-sequence 2)})]
      (append-orders! [live-id])
      (is (= (conj (subvec ids 2) live-id)
             (order-ids (receive-all c))))))

  (testing "a start sequence of 0 replays everything"
    (let [ids (vec (repeatedly 3 random-uuid))
          _ (append-orders! ids)
          c (l/subscribe *event-store* (orders-query ids) {:start-sequence 0})]
      (is (= ids (order-ids (receive-all c))))))

  (testing "replays more events than the channel can hold"
    (let [ids (vec (repeatedly 300 random-uuid))
          _ (append-orders! ids)
          c (l/subscribe *event-store* (orders-query ids) {:start-sequence 0})]
      (is (= ids (order-ids (receive-all c)))))))

(deftest subscribe-closing
  (testing "closing the channel while a put waits for room ends the subscription"
    (let [ids (vec (repeatedly 100 random-uuid))
          _ (append-orders! ids)
          c (l/subscribe *event-store* (orders-query ids) {:start-sequence 0})]
      ;; Nothing is taken from c, so the feeding thread waits for room
      (Thread/sleep 200)
      (async/close! c)
      ;; Only the 10 buffered events and the one put pending at closing
      ;; are left, nothing more is put
      (is (= (take 11 ids) (order-ids (receive-all c)))))))
