(ns rp.kafka.kafka-transformer
  "Clojure wrapper for use with the Stream DSL `.transform` method via the DSL component's `transform` function.
  Wraps a record-callback (that takes one input record and returns a collection of 0-N output records.
  Supports an optional state store which can be accessed in record-callback using the KVStore protocol from rp.kafka.kafka-state-store.
  Usage:
  Inside your Stream processor's process-input-stream function you can call
  rp.kafka.kafka-stream-processor/transform
  passing the component and a keyword identifying one of the
  transformers you define in the component's `transformers` config map.
  Each entry in that is a submap with the keys:
  :output-key-schema - Avro schema for output record keys
  :output-value-schema - Avro schema for output record values
  :state-store - Name for the state store to create/use (optional)
  :record-callback - A function that takes a map representing an input record with the keys [:k :v :meta :component :state-store] and returns a sequence of output records each represented by a map with the keys [:k :v]."
  (:require [clojure.tools.logging :as log]
            [rp.kafka.avro :as avro]
            [rp.kafka.common :as common])
  (:import [org.apache.kafka.streams.kstream Transformer TransformerSupplier]
           [org.apache.kafka.streams.processor ProcessorContext]))

(defn str-array
  "Helper for passing a vararg of Strings to a Java method."
  [& strings]
  (into-array String strings))

(defn transform-record
  [component transformer-config ^ProcessorContext context state-store {:keys [k v meta] :as rec}]
  (let [{:keys [record-callback output-key-schema output-value-schema]} transformer-config]
    (log/info {:rec rec})               ; FIXME: del eventually
    (try
      (let [output-recs (record-callback {:k (and k (avro/->clj k))
                                          :v (and v (avro/->clj v))
                                          :meta meta
                                          :component component
                                          :state-store state-store})]
        (doseq [{:keys [k v] :as rec-out} output-recs]
          (log/info {:rec-out rec-out}) ; FIXME: del eventually
          (.forward context
                    (and k (avro/->java output-key-schema k))
                    (and v (avro/->java output-value-schema v)))))
      (catch Throwable t
        (common/report-throwable component t "Caught exception handling record" {:rec rec})))))

(deftype CustomTransformer [component
                            transformer-config
                            ^{:volatile-mutable true} ^ProcessorContext context
                            ^{:volatile-mutable true} state-store]
  Transformer

  (^void init [this ^ProcessorContext c]
   (set! context c)
   (when-let [store-name (:store-name transformer-config)]
     (set! state-store (.getStateStore context store-name))))

  (transform [this k v]
    (let [meta {:topic (.topic context)
                :partition (.partition context)
                :offset (.offset context)
                :timestamp (.timestamp context)}
          rec {:k k :v v :meta meta}]
      (transform-record component transformer-config context state-store rec)))

  (close [this]
    ;; No-op (just defined to satisfy interface)
    ;; Used to close state store here, but learned the hard way that we shouldn't bother to close the store.
    ;; https://issues.apache.org/jira/browse/KAFKA-4919
    ))

(defn transformer-supplier
  [component transformer-key]
  (let [transformer-config (get-in component [:transformers transformer-key])]
    (assert transformer-config (str "Missing transformer config for " (pr-str transformer-key)))
    (reify TransformerSupplier
      (get [_] (->CustomTransformer component transformer-config nil nil)))))
