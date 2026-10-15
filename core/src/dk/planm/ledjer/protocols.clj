(ns dk.planm.ledjer.protocols)

(defprotocol EventStore
  :extend-via-metadata true
  (-source [this query opts])
  (-subscribe [this query])
  (-append [this events condition]))
