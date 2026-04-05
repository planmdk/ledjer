(ns dk.planm.ledjer.event)

(defonce ^{:private true}
  event-tag-registry (atom {}))

(defn register-event-tags
  [event-type tags]
  (swap! event-tag-registry
         (fn [registry et ts]
           (when (get registry et)
             (tap> ["event-type already registered" {:event/type et}]))
           (assoc registry et ts))
         event-type
         tags))

(defn payload
  [event]
  (:event/payload event))

(defn metadata
  [event]
  (:event/metadata event))

(defn payload-path
  "Build path to value in an event's payload."
  [kw]
  [:event/payload kw])

(defn metadata-path
  "Build path to value in an event's metadata."
  [kw]
  [:event/metadata kw])

(defn get-event-tags
  ([event-type]
   (get-event-tags event-type @event-tag-registry))
  ([event-type registry]
   (get registry event-type)))

(defn event-type
  [event]
  (:event/type event))

(defn event->tag-bindings
  [event]
  (let [tags (event/get-event-tags (event/event-type event))]
    (reduce
     (fn [acc tag]
       (assoc acc tag (get-in event tag)))
     {}
     tags)))

(defn has-bindings?
  "Test if event has values matching the tag-bindings map, respecting wildcards."
  [event tag-bindings]
  (reduce
   (fn [acc [k v]]
     (cond
       (= "*" v)
       acc

       (= ::not-found (get-in event k ::not-found))
       acc

       (= v (get-in event k))
       true

       :else
       (reduced false)))
   true
   tag-bindings))
