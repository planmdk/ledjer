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
             path-item))
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
                    (flatten))]
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
     appended-events)))

(defn- in-memory-subscribe
  [event-store query]
  (let [c (async/chan 10)]
    (add-watch (:last-sequence-number event-store)
               :w
               (fn [_key _ref _previous-sequence-num current-sequence-num]
                 (let [latest-event (specter/select-one
                                     (specter/walker #(= (sequence-num %) current-sequence-num))
                                     @(:events event-store))]
                   (doseq [{:keys [event-types tag-bindings]} query]
                     (when (and (get event-types (event/event-type latest-event))
                                (event/has-bindings? latest-event tag-bindings))
                       (println "found event")
                       (when-not (async/put! c latest-event)
                         (println "removing watch")
                         (remove-watch (:event-counter event-store) :w)))))))
    c))

(defn in-memory-event-store
  []
  (with-meta
    {:events (ref {})
     :last-sequence-number (ref 0)}
    {`p/-source #'in-memory-source
     `p/-append #'in-memory-append
     `p/-subscribe #'in-memory-subscribe}))
