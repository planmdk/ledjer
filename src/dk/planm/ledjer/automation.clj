(ns dk.planm.ledjer.automation
  (:require
   [clojure.core.async :as async]))

(defn periodic
  "Helper to create periodic ticks, useful as triggers for automations."
  [ms]
  (let [out-c (async/chan 1)]
    (async/go-loop []
      (let [_ (async/<! (async/timeout ms))]
        (when (async/>! out-c :tick)
          (recur))))
    out-c))
