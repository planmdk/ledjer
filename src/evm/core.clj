(ns evm.core
  (:require
   [clojure.string :as string]
   [clojure.core.async :as async]
   [com.rpl.specter :as specter]
   [malli.core :as malli]
   [malli.error :as merror]
   [clojure.test :as test])
  (:import
   [clojure.core.async.impl.channels ManyToManyChannel]))

(defonce event-tag-registry (atom {}))

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

(defmacro defevent
  {:clj-kondo/lint-as 'clojure.core/def}
  [name & {:keys [type version tags schema]}]
  (when-not type (throw (ex-info "Event definition must declare a type." {})))
  (when-not schema (throw (ex-info "Event definition must declare a schema." {})))
  (let [v (or version 1)
        tags (or tags [])]
    `(do
       (register-event-tags ~type ~tags)
       (defn ~name
         ([] ~type)
         ([payload#] (~name {} payload#))
         ([metadata# payload#]
          (try
            (malli/assert ~schema payload#)
            {:event/type ~type
             :event/metadata (merge metadata# {:version ~v})
             :event/payload payload#}
            (catch Exception e#
              (throw (ex-info "Event does not match schema."
                              (-> e# ex-data :data :explain malli.error/humanize))))))))))

(defn get-event-tags
  ([event-type]
   (get-event-tags event-type @event-tag-registry))
  ([event-type registry]
   (get registry event-type)))

(defn event-type
  [event]
  (:event/type event))

(defn- tag-path->string
  [tag-path]
  ;; (assert (= 2 (count tag-path)) "tag-path must be a pair")
  (let [kw (last tag-path)]
    (if-let [prefix (namespace kw)]
      (str prefix "__" (name kw))
      (name kw))))

(defn event-path-vec
  [event-type tag-bindings]
  (let [tags (get-event-tags event-type)
        tags-with-data (mapcat
                        (fn [tag]
                          [(tag-path->string tag) (get tag-bindings tag "*")])
                        tags)]
    (into [(tag-path->string [event-type])] tags-with-data)))

(defn event->tag-bindings
  [event]
  (let [tags (get-event-tags (event-type event))]
    (reduce
     (fn [acc tag]
       (assoc acc tag (get-in event tag)))
     {}
     tags)))

(defn event-append-path
  "Compute the file system path for the event's stream."
  [event]
  (string/join "/" (event-path-vec (event-type event) (event->tag-bindings event))))

(defn event-read-path
  "Compute the file system path for reading the event stream of event-type. Can contain globs."
  [event-type tag-bindings]
  (string/join "/" (event-path-vec event-type tag-bindings)))

(defn event-append-subject
  "Compute the NATS subject for the event."
  [event]
  (string/join "." (event-path-vec (:event/type event) (event->tag-bindings event))))

(defn event-read-subject
  "Compute the NATS subject filter for the event type. Can contain wildcards."
  [event-type tag-bindings]
  (string/join "." (event-path-vec event-type tag-bindings)))

(defprotocol EventStore
  :extend-via-metadata true
  (-source [this event-types tag-bindings opts])
  (-subscribe [this event-types tag-bindings])
  (-append [this event]))

(defn source [event-store event-types tag-bindings & opts]
  (-source event-store event-types tag-bindings opts))

(defn subscribe [event-store event-types tag-bindings]
  (-subscribe event-store event-types tag-bindings))

(defn append [event-store event]
  (-append event-store event))

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
                     event-types)]
    (let [events (->> event-paths
                      (mapcat (fn [path] (get-in* all-events path)))
                      (filter (comp not nil?))
                      (flatten)
                      (sort-by (fn [ev] (get-in ev [:event/metadata :sequence]))))]
      (if start-sequence
        (into [] (drop-while (fn [e] (< (:sequence (metadata e)) start-sequence))) events)
        events))))

(defn- in-memory-append
  [event-store event]
  (let [event-path (event-path-vec (:event/type event) (event->tag-bindings event))]
    (dosync
     (let [e (assoc-in event [:event/metadata :sequence] @(:event-counter event-store))]
       (alter (:events event-store)
              update-in
              event-path
              (fnil conj [])
              e)
       (alter (:event-counter event-store) inc)
       e))))

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
                              (has-bindings? latest-event tag-bindings))
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
    {`-source #'in-memory-source
     `-append #'in-memory-append
     `-subscribe #'in-memory-subscribe}))

(defn- binding-pair-type
  "Get the event type from a state view binding pair."
  [binding-pair]
  (let [[kw-or-vec _] binding-pair]
    (if (keyword? kw-or-vec)
      kw-or-vec
      (first kw-or-vec))))

(defn- binding-pair-tags
  "Get the tag bindings from a state view binding pair."
  [binding-pair]
  (let [[kw-or-vec _] binding-pair]
    (if (keyword? kw-or-vec)
      {}
      (second kw-or-vec))))

(defn- binding-pair-reducer
  "Get the event reducer from a state view binding pair."
  [binding-pair]
  (let [[_ reducer] binding-pair]
    reducer))

(defn- binding-pair-reducer-for
  "Get the event reducer for event from the state view binding-pairs."
  [binding-pairs event]
  (let [pairs-matching-type (into {}
                                  (comp
                                   (filter (fn [pair]
                                             (= (binding-pair-type pair) (event-type event))))
                                   (filter (fn [pair]
                                             (has-bindings? event (binding-pair-tags pair)))))
                                  binding-pairs)]
    (if (not= 1 (count pairs-matching-type))
      (throw (ex-info "Expected exactly one matching binding pair per event!" {:num-matching (count pairs-matching-type)}))
      (binding-pair-reducer (first pairs-matching-type)))))

(defn view*
  "Given a map of binding-pairs, either kw -> reducer-fn or [kw
  tag-bindings] -> reducer-fn, and a map of tag-bindings, reduces
  events sourced from event-store to a single value using the
  reducer-fns.

  Will throw in case an event is sourced for which there is no
  reducer-fn registered."
  [event-store binding-pairs tag-bindings initial-acc]
  (let [events (source event-store (into #{} (map binding-pair-type) binding-pairs) tag-bindings)]
    (reduce
     (fn [acc event]
       (let [event-reducer (binding-pair-reducer-for binding-pairs event)]
         (event-reducer acc event)))
     initial-acc
     events)))

(defmacro defview
  "Define a state view, a function of an event-store and a map of tag
  bindings."
  {:clj-kondo/lint-as 'clojure.core/def}
  [name initial-acc & binding-pairs]
  (assert (even? (count binding-pairs)) "defview requires an even number of binding pairs.")
  (let [binding-pairs-seq (into [] (partitionv 2 binding-pairs))]
    `(do
       (defn
         ^{:state-view true}
         ~(vary-meta name merge {:state-view true})
         [event-store# tag-bindings#]
         (view* event-store# ~binding-pairs-seq tag-bindings# ~initial-acc)))))

(defn periodic
  "Helper to create periodic ticks, useful as triggers for automations."
  [ms]
  (let [out-c (async/chan 1)]
    (async/go-loop []
      (let [_ (async/<! (async/timeout ms))]
        (when (async/>! out-c :tick)
          (recur))))
    out-c))

(defn- channel?
  [v]
  (instance? ManyToManyChannel v))

(defn automation
  "Given a set of triggers, a map of inputs, and a change fn, returns a
  function that takes an event-store and starts listening for the
  triggers. Triggers can be any combination of event types, event
  type/tag-binding pairs, and core.async channels.

  When a trigger occurs, the change fn is called with the event-store
  and the map of inputs."
  [event-store triggers body-fn]
  (let [trigger-events (into #{} (filter keyword? triggers))
        trigger-event-tag-pairs (filter vector? triggers)
        trigger-channels (filter channel? triggers)
        stop-chan (async/chan 1)
        trigger-chans (-> #{}
                          (conj (subscribe event-store trigger-events {}))
                          (into (map (fn [[event-type tag-bindings]]
                                       (subscribe event-store event-type tag-bindings))
                                     trigger-event-tag-pairs))
                          (into trigger-channels)
                          (conj stop-chan))]
    (println trigger-events)
    (println trigger-event-tag-pairs)
    (println trigger-channels)
    (println trigger-chans)
    (async/go-loop [[_ p] (async/alts! trigger-chans)]
      (println (str "automation: " p))
      (tap> [:automation-triggered])
      (when-not (= p stop-chan)
        (body-fn event-store)
        (recur (async/alts! trigger-chans))))
    (fn []
      (async/close! stop-chan))))

(defn stop-automation
  [automation]
  (async/close! automation))

(defn gwt*
  [& {:keys [given when then]}]
  (assert (var? (first when)) "The first element of the :when must be a var. Did you forget to prepend #' or wrap it in (var)?")
  (let [event-store (in-memory-event-store)
        [when-fn & payload] when
        start-sequence (atom 0)]
    (doseq [g given]
      (let [e (append event-store g)]
        (reset! start-sequence (:sequence (metadata e)))))
    (if (instance? java.util.regex.Pattern then)
      (test/is (thrown-with-msg? clojure.lang.ExceptionInfo then (apply when-fn event-store payload)))
      (if (:state-view (meta when-fn))
        (test/is (= (apply when-fn event-store payload) then))
        (do
          (apply when-fn event-store payload)
          (let [new-events (source event-store (into #{} (map :event/type then)) {} :start-sequence @start-sequence)]
            (test/is (= (map (fn [e] (select-keys e #{:event/type :event/payload})) new-events)
                        (map (fn [e] (select-keys e #{:event/type :event/payload})) then)))))))))

(defn gt
  [& {:keys [automation given then timeout-ms]}]
  (let [event-store (in-memory-event-store)
        automation-stop-fn (automation event-store)]
    (try
      (doseq [g given]
        (append event-store g))
      (let [timeout-ch (async/timeout (or timeout-ms 1000))
            new-events-ch (async/take
                           (count then)
                           (subscribe event-store (into #{} (map event-type then)) {}))
            go-ch (async/go-loop [[t & thens] then
                                  ev (async/<! new-events-ch)]
                    (when ev
                      (test/is (= (select-keys ev #{:event/type :event/payload})
                                  (select-keys t #{:event/type :event/payload})))
                      (when (seq thens)
                        (recur thens (async/<! new-events-ch)))))]
        (when (= :timed-out
                 (async/alt!!
                   timeout-ch :timed-out
                   go-ch :ran))
          (throw (ex-info "Given-Then timed out." {:timeout-ms timeout-ms}))))
      (finally
        (automation-stop-fn)))))

(comment

  (a-event)
  (a-event {:entity/id "foobar" :fo "one"})
  (a-event {:correlation-id 123} {:entity/id "foobar" :foo "one"})
  (event-append-path (a-event {} {:entity/id "foobar" :foo "one"}))
  (event-read-path (a-event) {(payload-path :entity/id) "test" (payload-path :foo) "notme"})
  (event-append-subject (a-event {} {:entity/id "foobar" :foo "one"}))
  (event-read-subject (a-event) {(payload-path :foo) "yesman"})
  (defevent a-event
    :type :test/a
    :version 1
    :tags [(metadata-path :version) (payload-path :entity/id) (payload-path :foo)]
    :schema [:map
             [:entity/id :string]
             [:foo :string]])

  (defevent a-event-v2
    :type :test/a
    :version 2
    :tags [(metadata-path :version) (payload-path :entity/id) (payload-path :foo)]
    :schema [:map
             [:entity/id :string]
             [:new-key :int]
             [:foo :string]])
  (defevent b-event
    :type :test/b
    :version 1
    :tags [(payload-path :entity/id)]
    :schema [:map
             [:entity/id :string]
             [:bar :string]])
  #__)

(comment
  (def evs5 (in-memory-event-store))
  (def sub-c (subscribe evs5 #{(a-event) (b-event)} {(payload-path :entity/id) "foobar" (payload-path :foo) "two" (payload-path :bar) "*"}))

  (def go-c (async/go-loop []
              (when-let [v (async/<! sub-c)]
                (println v)
                (recur))))
  (async/close! go-c)
  (async/close! sub-c)

  (append evs5 (a-event {:entity/id "foobar" :foo "one"}))
  (append evs5 (a-event {:entity/id "foobar" :foo "two"}))
  (append evs5 (b-event {:entity/id "foobar" :bar "snaz"}))
  (append evs5 (a-event {:entity/id "snaz" :foo "two"}))
  (append evs5 (a-event-v2 {:entity/id "foobar" :foo "one" :new-key 42}))

  (count (source evs5 #{(a-event)} {(payload-path :foo) "two"}))
  (count (source evs5 #{(a-event)} {}))
  )

(comment
  (defview some-view {}
    [(a-event) {(metadata-path :version) 1}]
    (fn a-reducer [acc _e]
      (update acc :count-v1 (fnil inc 0)))

    [(a-event) {(metadata-path :version) 2}]
    (fn a-reducer [acc _e]
      (update acc :count-v2 (fnil inc 0)))

    (b-event)
    (fn b-reducer [acc _e]
      (assoc acc :b-seen? true)))

  (some-view evs5 {})
  (meta #'some-view))

(comment
  (defn- test-change
    [event-store payload]
    (append event-store (b-event {:entity/id (str "test-" (:count payload)) :bar "nope"})))
  (def some-auto
    (automation evs5
                #{(a-event)}
                (fn [event-store]
                  (let [counts (some-view event-store {})]
                    (test-change event-store counts)))))

  (stop-automation some-auto)
  )

(comment
  (defn test-change
    [event-store payload]
    (when (= (:op payload) "combine")
      (let [a-events (source event-store #{(a-event)} {:entity/id (:entity/id payload)})
            the-bar (string/join "" (map (comp :foo :event/payload) a-events))]
        #_(throw (ex-info "Foobar" {}))
        (append event-store (b-event {:entity/id (:entity/id payload) :bar the-bar})))))

  (gwt*
   :given [(a-event {:entity/id "1" :foo "one"})
           (a-event {:entity/id "1" :foo "two"})]
   :when [test-change {:op "combine" :entity/id "1"}]
   :then #_#"Foobar"[(b-event {:entity/id "1" :bar "onetwo"})])


  (gwt*
   :given [(a-event {:entity/id "1" :foo "one"})]
   :when [some-view {}]
   :then {:count 1}))
