;; FIXME: Consider refactoring/deprecating; one could use Kstream's `process` method with a DSL stream processor instead.
;; See https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/KStream.html#process-org.apache.kafka.streams.processor.ProcessorSupplier-java.lang.String...-
(ns rp.kafka.kafka-stream-processor
  "A Stream Processor component (using the lower-level Processor API, not the DSL API).
  Note that (at least for now) this wraps a simple topology with a single source (which may be multiple topics), a single processor (with optional state store) and a single sink topic."
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [rp.kafka.avro :as avro]
            [rp.kafka.common :as common])
  (:import [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig Topology]
           [org.apache.kafka.streams.errors DeserializationExceptionHandler LogAndContinueExceptionHandler]
           [org.apache.kafka.streams.processor Processor ProcessorContext ProcessorSupplier]
           [org.apache.kafka.streams.state Stores]
           GenericPrimitiveAvroSerde
           [io.confluent.kafka.serializers AbstractKafkaAvroSerDeConfig KafkaAvroDeserializer]
           [java.util Properties]))

(defn str-array
  "Helper for passing a vararg of Strings to a Java method."
  [& strings]
  (into-array String strings))

(defn state-store-builder
  "Returns a builder for a persistent store with the specified name. The store keys and values are both strings."
  [store-name]
  (Stores/keyValueStoreBuilder
   (Stores/persistentKeyValueStore store-name)
   (Serdes/String)
   (Serdes/String)))

(defn process-record
  [component context state-store {:keys [k v meta] :as rec}]
  (let [{:keys [record-callback output-key-schema output-value-schema]} component]
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

(deftype CustomProcessor [component
                          ^{:volatile-mutable true} context
                          ^{:volatile-mutable true} state-store]
  Processor

  (^void init [this ^ProcessorContext c]
   (set! context c)
   (when-let [store-name (:store-name component)]
     (set! state-store (.getStateStore context store-name))))

  (process [this k v]
    (let [meta {:topic (.topic context)
                :partition (.partition context)
                :offset (.offset context)
                :timestamp (.timestamp context)}
          rec {:k k :v v :meta meta}]
      (process-record component context state-store rec)))

  (close [this]
    ;; No-op (just defined to satisfy interface)
    ;; Used to close state store here, but learned the hard way that we shouldn't bother to close the store.
    ;; https://issues.apache.org/jira/browse/KAFKA-4919
    ))

(defrecord KafkaStreamProcessor [bootstrap-servers schema-registry-url app-id input-topics output-topic output-key-schema output-value-schema record-callback store-name auto-register-schemas?]
  component/Lifecycle
  (start [this]
    ;; Ugh; KafkaStreams constructor requires Properties for config
    ;; (which limits us to only string values).
    (let [config (doto (Properties.)
                   (.setProperty StreamsConfig/APPLICATION_ID_CONFIG app-id)
                   (.setProperty StreamsConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
                   (.setProperty StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName GenericPrimitiveAvroSerde))
                   (.setProperty StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName GenericPrimitiveAvroSerde))
                   ;; Ideally we'd log deserialization errors to Sentry as well, but the use of string properties for config makes that convoluted.
                   ;; We could create a custom handler class, but we can't pass the error-handler object to it; the best we could do is pass the DSN string as custom config and use that to construct another error handler instance. IMO not worth the hassle.
                   (.setProperty StreamsConfig/DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG (.getName LogAndContinueExceptionHandler))
                   (.setProperty AbstractKafkaAvroSerDeConfig/SCHEMA_REGISTRY_URL_CONFIG schema-registry-url)
                   (.setProperty AbstractKafkaAvroSerDeConfig/AUTO_REGISTER_SCHEMAS (str (boolean auto-register-schemas?))))
          topology (-> (Topology.)
                       (.addSource "Source" (apply str-array input-topics))
                       (.addProcessor "Process"
                                      (reify ProcessorSupplier
                                        (get [_] (->CustomProcessor this nil nil)))
                                      (str-array "Source"))
                       (.addSink "Sink" output-topic (str-array "Process")))
          topology (cond-> topology
                     store-name (.addStateStore (state-store-builder store-name)
                                                (str-array "Process")))
          streams (KafkaStreams. topology config)]
      (.start streams)
      (assoc this :streams streams)))
  (stop [this]
    (try
      (.close (:streams this))
      (catch Throwable t
        (common/report-throwable this t "Caught exception closing streams" {:app-id app-id})))
    this))
