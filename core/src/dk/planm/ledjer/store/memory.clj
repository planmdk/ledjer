(ns dk.planm.ledjer.store.memory
  (:require
   [clojure.core.async :as async]
   [clojure.set :as set]
   [com.rpl.specter :as specter]
   [dk.planm.ledjer.event :as event]
   [dk.planm.ledjer.protocols :as p]))

(defn- tag-path->string
  [tag-path]
  (let [kw (last tag-path)]
    (if-let [prefix (namespace kw)]
      (str prefix "__" (name kw))
      (name kw))))

(defn- event-path-vec
  [event-type tag-bindings]
  (let [tags (event/get-event-tags event-type)
        tags-with-data (mapcat
                        (fn [tag]
                          [(tag-path->string tag) (get tag-bindings tag "*")])
                        tags)]
    (into [(tag-path->string [event-type])] tags-with-data)))

(defn- get-in*
  "Like get-in but supports wildcards (\"*\")"
  [m path]
  (specter/select
   (mapv (fn [path-item]
           (if (= "*" path-item)
             specter/MAP-VALS
             ;; Tag values can be of any type (e.g. UUIDs), and only
             ;; some types are implicit specter navigators.
             (specter/keypath path-item)))
         path)
   m))

(defn- sequence-num [event]
  (get-in event [:event/metadata :sequence]))

(defn- source-single-query
  [event-store query-map opts]
  (let [{:keys [start-sequence]} opts
        {:keys [event-types tag-bindings]} query-map
        all-events @(:events event-store)
        event-paths (map
                     (fn [event-type]
                       (event-path-vec event-type tag-bindings))
                     event-types)
        events (->> event-paths
                    (mapcat (fn [path] (get-in* all-events path)))
                    (filter (comp not nil?))
                    (flatten)
                    (sort-by #(sequence-num %)))]
    (if start-sequence
      (into #{} (drop-while (fn [e] (< (sequence-num e) start-sequence))) events)
      (into #{} events))))

(defn- in-memory-source
  [event-store query opts]
  (into []
        (sort-by sequence-num
                 (reduce (fn [acc query-map]
                           (into acc (source-single-query event-store query-map opts)))
                         #{}
                         query))))

(defn- in-memory-append
  [event-store events condition]
  (dosync
   (let [appended-events (doall
                          (for [event events]
                            (let [next-seq-num (alter (:last-sequence-number event-store) inc)
                                  e (assoc-in event [:event/metadata :sequence] next-seq-num)
                                  event-path (event-path-vec (:event/type event) (event/event->tag-bindings event))]
                              (alter (:events event-store)
                                     update-in
                                     event-path
                                     (fnil conj [])
                                     e)
                              e)))]
     (when (seq condition)
       (let [{:keys [query after]} condition
             events (p/-source event-store query (when after {:start-sequence after}))]
         (when (seq (set/difference (into #{} events) (into #{} appended-events)))
           (throw (ex-info "tx failed" {})))))
     (:sequence (event/metadata (last appended-events))))))

(defn- matches?
  "Whether event matches query-map, the same way source selects events:
  its type is one of the query map's event types, and each of its
  type's tags that is bound has the bound value, or is bound to \"*\".
  Bindings for tags the event type doesn't have don't apply to it."
  [event {:keys [event-types tag-bindings]}]
  (let [event-type (event/event-type event)]
    (and (contains? event-types event-type)
         (every? (fn [tag]
                   (let [bound (get tag-bindings tag "*")]
                     (or (= "*" bound)
                         (= bound (get-in event tag)))))
                 (event/get-event-tags event-type)))))

(defn- events-after
  "The events with a sequence number in (from, to], in order."
  [event-store from to]
  (->> @(:events event-store)
       (specter/select (specter/walker (fn [x]
                                         (when-let [n (and (map? x) (sequence-num x))]
                                           (< from n (inc to))))))
       (sort-by sequence-num)))

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

(defn- in-memory-subscribe
  "Subscribe to the events matching query.

  Without :start-sequence in opts, only events appended after
  subscribing are delivered. With it, the matching events from that
  sequence number (inclusive) on are delivered first: existing events
  are replayed on a separate thread, with backpressure, and then the
  subscription goes live. Events are delivered in order, each once."
  [event-store query opts]
  (let [sequence-ref (:last-sequence-number event-store)
        c (async/chan 10)
        ;; Each subscription needs its own watch key, or they would
        ;; replace each other
        watch-key (Object.)
        lock (Object.)
        ;; Events up to and including this sequence number have been
        ;; delivered, or are skipped
        delivered (volatile! 0)
        ;; Live subscriptions are delivered to by the watch, on the
        ;; thread of the append. Until then, the replay delivers.
        live? (volatile! false)
        end-subscription! (fn [] (remove-watch sequence-ref watch-key))
        matching-events (fn [from to]
                          (filter (fn [event] (some #(matches? event %) query))
                                  (events-after event-store from to)))
        deliver-live! (fn []
                        (locking lock
                          (when @live?
                            (let [current @sequence-ref]
                              (loop [[event & events] (matching-events @delivered current)]
                                (when event
                                  (if (async/put! c event)
                                    (recur events)
                                    ;; The channel is closed: end the subscription
                                    (end-subscription!))))
                              ;; A start sequence in the future leaves
                              ;; delivered ahead of current
                              (vswap! delivered max current)))))
        replay! (fn []
                  (async/thread
                    (loop []
                      (let [current @sequence-ref]
                        ;; every? stops at the first closed channel
                        (if (every? #(put-or-closed! c %) (matching-events @delivered current))
                          (do
                            (vswap! delivered max current)
                            ;; Go live, unless events were appended
                            ;; while replaying
                            (when-not (locking lock
                                        (when (= current @sequence-ref)
                                          (vreset! live? true)))
                              (recur)))
                          (end-subscription!))))))]
    ;; A ref's watches are called once per transaction, so one append
    ;; of several events moves the sequence number from previous to
    ;; current in one step: deliver every event in between.
    (add-watch sequence-ref watch-key (fn [_key _ref _previous _current] (deliver-live!)))
    (if-let [start-sequence (:start-sequence opts)]
      (do
        (vreset! delivered (max 0 (dec start-sequence)))
        (replay!))
      (locking lock
        (vreset! delivered @sequence-ref)
        (vreset! live? true)))
    c))

(defn in-memory-event-store
  []
  (with-meta
    {:events (ref {})
     :last-sequence-number (ref 0)}
    {`p/-source #'in-memory-source
     `p/-append #'in-memory-append
     `p/-subscribe #'in-memory-subscribe}))
