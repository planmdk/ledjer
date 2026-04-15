(ns dk.planm.ledjer
  "# Ledjer

  A small event sourcing library aiming to minimize the gap between
  Event Models (https://eventmodeling.org/) and corresponding event
  sourced applications.

  The concepts in Event Modeling map almost directly to concepts in Ledjer:

  - state view: (defview ...)
  - automation: (automation ...)
  - event: (defevent ...)
  - state change: defn that either fails or appends events
  - GWT: (gwt ...)
  - GT: (gt ...)

  Ledjer is opinionated. "
  (:require
   [clojure.core.async :as async]
   [clojure.string :as string]
   [clojure.test :as test]
   [dk.planm.ledjer.event :as event]
   [dk.planm.ledjer.protocols :as p]
   [dk.planm.ledjer.store.memory :as store.memory]
   [dk.planm.ledjer.view]
   [malli.core :as malli]
   [malli.error :as merror])
  (:import
   [clojure.core.async.impl.channels ManyToManyChannel]))

(defmacro defevent
  "Define an event.

  type and schema are required, and type should be unique across calls
  to defevent. Schema must be a valid malli schema.

  Creates an event constructor function that validates payloads
  against the event's schema."
  {:clj-kondo/lint-as 'clojure.core/def}
  [name & {:keys [type version tags schema]}]
  (when-not type (throw (ex-info "Event definition must declare a type." {})))
  (when-not schema (throw (ex-info "Event definition must declare a schema." {})))
  (let [v (or version 1)
        tags (or tags [])]
    `(do
       (dk.planm.ledjer.event/register-event-tags ~type ~tags)
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

(defn source
  "Source the events matching event-types and tag-bindings from the
  event-store.

  Valid options:

  - start-sequence: The sequence number from which to start sourcing

  query is a set of maps with keys :event-types #{} and :tag-bindings
  {}. A query map describes the event types and tag bindings that must
  all match; and multiple query maps are joined by logical OR."
  [event-store query & opts]
  (p/-source event-store query opts))

(defn subscribe
  "Subscribe to new events arriving in the event-store which match
  event-types and tag-bindings.

  query is a set of maps with keys :event-types #{} and :tag-bindings
  {}. A query map describes the event types and tag bindings that must
  all match; and multiple query maps are joined by logical OR."
  [event-store query]
  (p/-subscribe event-store query))

(defn append
  "Append events to the event-store.

  The optional condition is a map with the keys:

  - query: a set of query maps as for source and subscribe

  - after: the sequence number at which to start looking for new
  events matching query

  Evaluates condition immediately before persisting events. Appending
  events will fail if any events are returned by the condition query.

  Returns the appended events."
  ([event-store events]
   (append event-store events nil))
  ([event-store events condition]
   (p/-append event-store events condition)))

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
         (dk.planm.ledjer.view/view* event-store# ~binding-pairs-seq tag-bindings# ~initial-acc)))))

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
                          (conj (subscribe event-store #{{:event-types trigger-events}}))
                          (into (map (fn [[event-type tag-bindings]]
                                       (subscribe event-store #{{:event-types #{event-type} :tags-bindings tag-bindings}}))
                                     trigger-event-tag-pairs))
                          (into trigger-channels)
                          (conj stop-chan))]
    (async/go-loop [[_ p] (async/alts! trigger-chans)]
      (tap> [:automation-triggered {:stop? (= p stop-chan)}])
      (when-not (= p stop-chan)
        (body-fn event-store)
        (recur (async/alts! trigger-chans))))
    (fn []
      (async/close! stop-chan))))

(defn gwt
  "Given-When-Then tests whether a state change or state view (when)
  result in the expected output(s) (then) given the input
  events (given).

  - given must be a vector of event types

  - when must be a vector where the first element is a var that
  contains the state view or state change function and the rest are
  extra arguments to the function

  - then can be an arbitrary value (if the var in when points to a
  state view), a vector of events (if the var in when points to a
  state change), or a regex (if the var points to a state change)

  When then is a regex, it means an exception is expected to be thrown
  with a message matching the regex.

  When then is a vector of events, it is expected that their types and
  payloads match those of the events resulting from the state change
  exactly, including order."
  [& {:keys [given when then]}]
  (assert (var? (first when)) "The first element of the :when must be a var. Did you forget to prepend #' or wrap it in (var)?")
  (let [event-store (store.memory/in-memory-event-store)
        [when-fn & payload] when
        start-sequence (atom 0)]
    (let [appended-events (append event-store given)]
      (reset! start-sequence (:sequence (event/metadata (last appended-events)))))
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
  "Given-Then tests whether an automation results in the expected
  outputs (then) given the input events.

  - automation must be a function returning an automation given an
  event-store

  - given must be a vector of events

  - then must be a vector of events

  - timeout-ms (optional) controls for how long the test will await
  the automation before timing out"
  [& {:keys [automation given then timeout-ms]}]
  (let [event-store (store.memory/in-memory-event-store)
        automation-stop-fn (automation event-store)]
    (try
      (append event-store given)
      (let [timeout-ch (async/timeout (or timeout-ms 1000))
            new-events-ch (async/take
                           (count then)
                           (subscribe event-store #{{:event-types (into #{} (map event/event-type then))}}))
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

  (defevent a-event
    :type :test/a
    :version 1
    :tags [(event/metadata-path :version)
           (event/payload-path :entity/id)
           (event/payload-path :foo)]
    :schema [:map
             [:entity/id :string]
             [:foo :string]])

  (defevent a-event-v2
    :type :test/a
    :version 2
    :tags [(event/metadata-path :version)
           (event/payload-path :entity/id)
           (event/payload-path :foo)]
    :schema [:map
             [:entity/id :string]
             [:new-key :int]
             [:foo :string]])
  (defevent b-event
    :type :test/b
    :version 1
    :tags [(event/payload-path :entity/id)]
    :schema [:map
             [:entity/id :string]
             [:bar :string]])

  (a-event)
  (a-event {:entity/id "foobar" :fo "one"})
  (a-event {:correlation-id 123} {:entity/id "foobar" :foo "one"})

  #__)

(comment
  (def evs5 (store.memory/in-memory-event-store))
  (def sub-c (subscribe evs5 #{(a-event) (b-event)} {(event/payload-path :entity/id) "foobar"
                                                     (event/payload-path :foo) "two"
                                                     (event/payload-path :bar) "*"}))

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

  (count (source evs5 #{(a-event)} {(event/payload-path :foo) "two"}))
  (count (source evs5 #{(a-event)} {}))
  )

(comment
  (defview some-view {}
    [(a-event) {(event/metadata-path :version) 1}]
    (fn a-reducer [acc _e]
      (update acc :count-v1 (fnil inc 0)))

    [(a-event) {(event/metadata-path :version) 2}]
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

  (some-auto)
  )

(comment
  (defn test-change
    [event-store payload]
    (when (= (:op payload) "combine")
      (let [a-events (source event-store #{(a-event)} {:entity/id (:entity/id payload)})
            the-bar (string/join "" (map (comp :foo :event/payload) a-events))]
        #_(throw (ex-info "Foobar" {}))
        (append event-store (b-event {:entity/id (:entity/id payload) :bar the-bar})))))

  (gwt
   :given [(a-event {:entity/id "1" :foo "one"})
           (a-event {:entity/id "1" :foo "two"})]
   :when [test-change {:op "combine" :entity/id "1"}]
   :then #_#"Foobar"[(b-event {:entity/id "1" :bar "onetwo"})])


  (gwt
   :given [(a-event {:entity/id "1" :foo "one"})]
   :when [some-view {}]
   :then {:count 1}))
