(ns rp.kafka.common
  (:require [clojure.tools.logging :as log]))

(defn report-throwable [component throwable & [msg extra]]
  "General handler for throwables that don't bubble up to the main thread.
  Components may opt to provide a :throwable-callback function to report
  these throwables (e.g. to Sentry)."
  ;; Optional args:
  ;; msg (string)
  ;; extra (map of misc extra info)
  (log/error throwable msg extra)
  (when-let [callback (:throwable-callback component)]
    (callback component throwable msg extra)))
