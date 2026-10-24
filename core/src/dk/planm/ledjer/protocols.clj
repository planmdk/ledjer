(ns dk.planm.ledjer.protocols)

(defprotocol EventStore
  :extend-via-metadata true
  (-source [this query opts])
  (-subscribe [this query opts])
  (-append [this events condition]))

(defprotocol PersistedAccumulator
  :extend-via-metadata true
  (-sequence-number
    [this]
    "The last applied sequence-number, `nil` if fresh accumulator.")

  (-tx-append
    [this op]
    "Append op to the open transaction. All ops appended this way
are committed as one unit using `-tx-commit`.

Must not do IO!")

  (-tx-commit
    [this sequence-number]
    "Commit the appended transaction ops along with the
sequence-number of the last handled event.

Once this returns, the committed state must be readable, and
`-sequence-number` must return sequence-number.

If this throws, the open transaction must be left intact: the
caller may retry the commit with the same sequence-number.")

  (-reset
    [this]
    "Reset accumulator to an empty state.

Must return the new accumulator."))

(defn persisted-accumulator?
  [x]
  (satisfies? PersistedAccumulator x))
