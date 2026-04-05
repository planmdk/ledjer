(ns dk.planm.ledjer.store.memory
  (:require
   [clojure.core.async :as async]
   [clojure.string :as string]
   [com.rpl.specter :as specter]
   [dk.planm.ledjer.event :as event]
   [dk.planm.ledjer.protocols :as p]))

(defn- tag-path->string
  [tag-path]
  (let [kw (last tag-path)]
    (if-let [prefix (namespace kw)]
      (str prefix "__" (name kw))
      (name kw))))

(defn event-path-vec
  [event-type tag-bindings]
  (let [tags (event/get-event-tags event-type)
        tags-with-data (mapcat
                        (fn [tag]
                          [(tag-path->string tag) (get tag-bindings tag "*")])
                        tags)]
    (into [(tag-path->string [event-type])] tags-with-data)))

(defn event-append-path
  "Compute the file system path for the event's stream."
  [event]
  (string/join "/" (event-path-vec (event/event-type event) (event/event->tag-bindings event))))

(defn event-read-path
  "Compute the file system path for reading the event stream of event-type. Can contain globs."
  [event-type tag-bindings]
  (string/join "/" (event-path-vec event-type tag-bindings)))

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

(defn- in-memory-source
  [event-store event-types tag-bindings opts]
  (let [{:keys [start-sequence]} opts
        all-events @(:events event-store)
        event-paths (map
                     (fn [event-type]
                       (event-path-vec event-type tag-bindings))
                     event-types)
        events (->> event-paths
                      (mapcat (fn [path] (get-in* all-events path)))
                      (filter (comp not nil?))
                      (flatten)
                      (sort-by (fn [ev] (get-in ev [:event/metadata :sequence]))))]
    (if start-sequence
      (into [] (drop-while (fn [e] (< (:sequence (event/metadata e)) start-sequence))) events)
      events)))

(defn- in-memory-append
  [event-store event]
  (let [event-path (event-path-vec (:event/type event) (event/event->tag-bindings event))]
    (dosync
     (let [e (assoc-in event [:event/metadata :sequence] @(:event-counter event-store))]
       (alter (:events event-store)
              update-in
              event-path
              (fnil conj [])
              e)
       (alter (:event-counter event-store) inc)
       e))))

(defn- sequence-num [event]
  (get-in event [:event/metadata :sequence]))

(defn- in-memory-subscribe
  [event-store event-types tag-bindings]
  (let [c (async/chan 10)]
    (add-watch (:event-counter event-store)
               :w
               (fn [_key _ref current-sequence-num _next-sequence-num]
                 (let [latest-event (specter/select-one
                                     (specter/walker #(= (sequence-num %) current-sequence-num))
                                     @(:events event-store))]
                   (when (and (get event-types (:event/type latest-event))
                              (event/has-bindings? latest-event tag-bindings))
                     (println "found event")
                     (when-not (async/put! c latest-event)
                       (println "removing watch")
                       (remove-watch (:event-counter event-store) :w))))))
    c))

(defn in-memory-event-store
  []
  (with-meta
    {:events (ref {})
     :event-counter (ref 0)}
    {`p/-source #'in-memory-source
     `p/-append #'in-memory-append
     `p/-subscribe #'in-memory-subscribe}))
