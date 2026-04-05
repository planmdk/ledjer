(ns dk.planm.ledjer.view
  (:require
   [dk.planm.ledjer.event :as event]
   [dk.planm.ledjer.protocols :as p]))

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
                                             (= (binding-pair-type pair) (event/event-type event))))
                                   (filter (fn [pair]
                                             (event/has-bindings? event (binding-pair-tags pair)))))
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
  (let [events (p/-source event-store (into #{} (map binding-pair-type) binding-pairs) tag-bindings {})]
    (reduce
     (fn [acc event]
       (let [event-reducer (binding-pair-reducer-for binding-pairs event)]
         (event-reducer acc event)))
     initial-acc
     events)))
