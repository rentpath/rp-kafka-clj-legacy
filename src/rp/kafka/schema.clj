(ns rp.kafka.schema
  (:require [cheshire.core :as json]
            [rp.kafka.avro :as avro]))

(defn fetch-schema
  [registry-url subject version]
  (-> (format "%s/subjects/%s/versions/%s" registry-url subject version)
      slurp
      (json/parse-string true)
      :schema
      avro/parse-schema))
