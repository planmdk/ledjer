(ns dk.planm.ledjer.query
  "Queries select events from an event store.

  A query is a set of query maps, joined by logical OR. A query map
  has the keys:

  - :event-types, a set of event types
  - :tag-bindings (optional), a map from tag path to value

  An event matches a query map when its type is one of the event types
  and, for each tag binding that names one of the event type's tags,
  the event's value at that tag equals the bound value. Values are
  compared by type and value, so the string \"1\" does not match the
  number 1, and a UUID does not match its string representation.

  The string \"*\" matches any value. nil is not allowed as a value:
  use \"*\" to match any value, or leave the binding out."
  (:require
   [dk.planm.ledjer.event :as event]
   [malli.core :as m]
   [malli.error :as me]))

(def TagPath
  [:tuple [:enum :event/payload :event/metadata] :keyword])

(def QueryMap
  [:map {:closed true}
   [:event-types [:set :keyword]]
   [:tag-bindings {:optional true}
    [:maybe
     [:map-of
      TagPath
      [:fn {:error/message "must not be nil; use \"*\" to match any value"} some?]]]]])

(def Query
  [:set QueryMap])

(def ^:private valid-shape? (m/validator Query))

(defn- unknown-tag-paths
  "Tag paths bound in query-map that are not a tag of any of its event
  types. Bindings for tags an event type doesn't have are ignored for
  that type, but a binding that applies to none of them is a mistake."
  [{:keys [event-types tag-bindings]}]
  (let [known (into #{} (mapcat event/get-event-tags) event-types)]
    (remove known (keys tag-bindings))))

(defn validate!
  "Throw if query is not a valid query, otherwise return it."
  [query]
  (when-not (valid-shape? query)
    (throw (ex-info "Invalid ledjer query"
                    {:query query
                     :errors (me/humanize (m/explain Query query))})))
  (doseq [query-map query
          :let [unknown (unknown-tag-paths query-map)]
          :when (seq unknown)]
    (throw (ex-info "Invalid ledjer query: tag binding for a tag none of the event types have"
                    {:query query
                     :event-types (:event-types query-map)
                     :unknown-tag-paths (vec unknown)})))
  query)
