(ns dk.planm.ledjer.store.umadb
  (:require
   [clojure.core.async :as async]
   [deed.core :as deed]
   [dk.planm.ledjer.event :as le]
   [dk.planm.ledjer.protocols :as p])
  (:import
   [io.grpc Context Context$CancellableContext]
   [io.umadb.client
    AppendCondition
    AppendRequest
    Event
    Query
    QueryItem
    ReadRequest
    ReadResponse
    SequencedEvent
    SubscribeRequest
    SubscribeResponse
    UmaDbClient]))

(defn- event-type->string
  [event-type-kw]
  (if-let [prefix (namespace event-type-kw)]
      (str prefix "__" (name event-type-kw))
      (name event-type-kw)))

(defn- tag-path->string
  [tag-path]
  (let [kw (last tag-path)]
    (if-let [prefix (namespace kw)]
      (str prefix "__" (name kw))
      (name kw))))

(defn- tag-value->string
  "Encode a tag value together with its type, so that values only match
  when both type and value are equal, as in the in-memory store. E.g. a
  UUID never matches its string representation."
  [v]
  (cond
    (uuid? v) (str "uuid:" v)
    (string? v) (str "string:" v)
    (keyword? v) (str "keyword:" (subs (str v) 1))
    (integer? v) (str "int:" v)
    :else (str (.getName (class v)) ":" v)))

(defn- event-tags
  "The UmaDB tags for event-type given tag-bindings. Only the event
  type's own tags are used, and bindings without a value are left
  out. Use query-tags for queries."
  [event-type tag-bindings]
  (into []
        (comp
         (map (juxt tag-path->string (partial get tag-bindings)))
         (filter (fn [[_ v]] (some? v)))
         (map (fn [[tag v]] (str tag ":" (tag-value->string v)))))
        (le/get-event-tags event-type)))

(defn- query-tags
  "Like event-tags, but also leaves out the \"*\" wildcard, as it
  matches any value."
  [event-type tag-bindings]
  (event-tags event-type (into {} (remove (fn [[_ v]] (= "*" v))) tag-bindings)))

(defn- ledjer-query->uma-query
  "One query item per event type, as each event type has its own tags.
  Returns nil for a query that matches no event types."
  [query]
  (let [query-items (for [{:keys [event-types tag-bindings]} query
                          event-type event-types]
                      (QueryItem/of [(event-type->string event-type)]
                                    (query-tags event-type tag-bindings)))]
    (when (seq query-items)
      (Query/of ^java.util.List (vec query-items)))))

(defn- uma-event->ledjer-event
  [^SequencedEvent umadb-event]
  (-> umadb-event
      (.event)
      (.data)
      (deed/decode-from)
      (assoc-in [:event/metadata :sequence] (.position umadb-event))))

(defn- uma-append
  [^UmaDbClient umadb-client events condition]
  (let [umadb-events (for [event events]
                       (let [event-tag-bindings (le/event->tag-bindings event)
                             event-type (le/event-type event)
                             tags (event-tags event-type event-tag-bindings)]
                         (Event/of (event-type->string event-type)
                                   tags
                                   (deed/encode-to-bytes event))))
        umadb-condition (when-let [query (and condition
                                              (ledjer-query->uma-query (:query condition)))]
                          (let [after-position (or (:after condition) (.getHeadPosition umadb-client))]
                            (AppendCondition/failIfExistsAfter query after-position)))
        umadb-append-request (AppendRequest. umadb-events umadb-condition)
        umadb-append-response (.handle umadb-client umadb-append-request)]
    (.position umadb-append-response)))

(defn- uma-source
  [^UmaDbClient umadb-client query opts]
  (let [umadb-query (when query (ledjer-query->uma-query query))]
    (if (and query (nil? umadb-query))
      [] ;; no event types, so nothing can match
      (let [start-position (or (:start-sequence opts) 0)
            umadb-read-request (ReadRequest.
                                umadb-query
                                start-position
                                false ;; forward from start-position
                                nil ;; unlimited number of events returned
                                nil) ;; default batch size
            umadb-read-iterator (.handle umadb-client umadb-read-request)
            umadb-read-responses (iterator-seq umadb-read-iterator)
            umadb-events (mapcat ReadResponse/.events umadb-read-responses)]
        (mapv uma-event->ledjer-event umadb-events)))))

(defn- put-or-closed!
  "Put val on c, waiting for room in it. Returns false if c is closed.

  Closing c doesn't release a put that is waiting for room, so the wait
  is cut short regularly to look for that."
  [c val]
  (loop []
    (let [[ret port] (async/alts!! [[c val] (async/timeout 50)] :priority true)]
      (if (= port c)
        ret
        ;; offer! returns nil if there is no room yet, false if closed
        (let [offered (async/offer! c val)]
          (if (nil? offered)
            (recur)
            offered))))))

(defn- uma-subscribe*
  "Feed events matching umadb-query with a position after `after` into c.

  The client derives each stream's gRPC context from the current one,
  so the subscription is opened inside a context of our own, which is
  cancelled to release the stream once c is closed."
  [^UmaDbClient umadb-client c umadb-query after]
  (let [^Context$CancellableContext context (.withCancellation (Context/current))
        umadb-subscribe-request (SubscribeRequest.
                                 umadb-query
                                 after
                                 nil) ;; default batch size
        ^java.util.Iterator umadb-subscription
        (.call context ^java.util.concurrent.Callable
               (fn [] (.subscribe umadb-client umadb-subscribe-request)))
        cancelled? (volatile! false)
        cancel! (fn []
                  (vreset! cancelled? true)
                  (.cancel context nil))]
    (async/io-thread
     (loop []
       (when-let [item (try
                         (when (.hasNext umadb-subscription)
                           (.next umadb-subscription))
                         (catch Exception e
                           ;; Closing c would make consumers like
                           ;; automations spin on the closed channel,
                           ;; so the error is reported and the
                           ;; subscription just stops. Cancellation is
                           ;; how it ends when the client shuts down.
                           (when-not (or @cancelled?
                                         (re-find #"CANCELLED" (str (ex-message e))))
                             (binding [*out* *err*]
                               (println "UmaDB subscription stopped:" (ex-message e))))
                           nil))]
         (if (every? #(put-or-closed! c %) (.events ^SubscribeResponse item))
           (recur)
           (cancel!)))))
    c))

(defn- uma-subscribe
  "Subscribe to the events matching query.

  Without :start-sequence in opts, only events appended after
  subscribing are delivered. With it, the matching events from that
  sequence number (inclusive) on are delivered first, then the
  subscription goes live."
  [^UmaDbClient umadb-client query opts]
  (let [c (async/chan 10 (map uma-event->ledjer-event))
        umadb-query (when query (ledjer-query->uma-query query))]
    (if (and query (nil? umadb-query))
      c ;; no event types, so nothing will ever arrive
      (let [after (if-let [start-sequence (:start-sequence opts)]
                    ;; UmaDB's after is exclusive, and positions start at 1
                    (max 0 (dec start-sequence))
                    (.getHeadPosition umadb-client))]
        (uma-subscribe* umadb-client c umadb-query (long after))))))

(extend-type UmaDbClient
  p/EventStore
  (-source [this query opts]
    (uma-source this query opts))

  (-subscribe [this query opts]
    (uma-subscribe this query opts))

  (-append [this events condition]
    (uma-append this events condition)))

(defn umadb-event-store
  "Build an event store backed by UmaDB.

  Options are:

  - :host
  - :port
  - :tls?
  - :certificate
  - :api-key"
  [opts]
  (let [{:keys [host port tls? _certificate _api-key] :or {host "localhost"
                                                           port 50051
                                                           tls? false}} opts
        umadb-client (let [builder (doto (UmaDbClient/builder)
                                     (.withHost host)
                                     (.withPort port))]

                       (when tls? (.withTlsEnabled builder))
                       ;; TODO: Handle certificate and api-key
                       (.build builder))]
    umadb-client))

(defn close-umadb-client
  [^UmaDbClient umadb-client]
  (.shutdown umadb-client))
