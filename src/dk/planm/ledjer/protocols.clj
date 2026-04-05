(ns dk.planm.ledjer.protocols)

(defprotocol EventStore
  :extend-via-metadata true
  (-source [this event-types tag-bindings opts])
  (-subscribe [this event-types tag-bindings])
  (-append [this event]))
