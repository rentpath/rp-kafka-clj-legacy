;; FIXME: If I del kafka-stream-processor (low), rename this without `dsl-`
(ns rp.kafka.kafka-stream-dsl-processor
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [rp.kafka.avro :as avro]
            [rp.kafka.common :as common]
            [rp.kafka.kafka-state-store :as store]
            [rp.kafka.kafka-transformer :as transformer])
  (:import [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig Topology KeyValue]
           [org.apache.kafka.streams.errors DeserializationExceptionHandler LogAndContinueExceptionHandler]
           [org.apache.kafka.streams.processor Processor ProcessorContext ProcessorSupplier]
           [org.apache.kafka.streams.kstream KStream Printed Initializer Aggregator Merger TimeWindows SessionWindows KeyValueMapper Suppressed Suppressed$BufferConfig Materialized Predicate]
           GenericPrimitiveAvroSerde
           [io.confluent.kafka.serializers AbstractKafkaAvroSerDeConfig KafkaAvroDeserializer]
           [java.util Properties]))

;;
;; Handy stuff "borrowed" from https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/streams/lambdas.clj
;;

(defn key-value
  "A key-value pair defined for a single Kafka Streams record."
  [[key value]]
  (KeyValue. key value))

(deftype FnInitializer [initializer-fn]
  Initializer
  (apply [this]
    (initializer-fn)))

(defn initializer
  "Packages up a Clojure fn in a kstream Initializer."
  ^Initializer [initializer-fn]
  (FnInitializer. initializer-fn))

(deftype FnAggregator [aggregator-fn]
  Aggregator
  (apply [this agg-key value aggregate]
    (aggregator-fn aggregate [agg-key value])))

(defn aggregator
  "Packages up a Clojure fn in a kstream Aggregator."
  ^Aggregator [aggregator-fn]
  (FnAggregator. aggregator-fn))

(deftype FnKeyValueMapper [key-value-mapper-fn]
  KeyValueMapper
  (apply [this key value]
    (key-value (key-value-mapper-fn [key value]))))

(defn key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper."
  [key-value-mapper-fn]
  (FnKeyValueMapper. key-value-mapper-fn))

(deftype FnPredicate [predicate-fn]
  Predicate
  (test [this key value]
    (boolean (predicate-fn [key value]))))

(defn predicate
  "Packages up a Clojure fn in a kstream predicate."
  [predicate-fn]
  (FnPredicate. predicate-fn))

;;
;; End of jackdaw stuff
;;

;; Note: Jackdaw doesn't include a wrapper for Merger (for aggregating session windows), so let's add it.
(deftype FnMerger [merger-fn]
  Merger
  (apply [this agg-key aggregate1 aggregate2]
    (merger-fn agg-key aggregate1 aggregate2)))

(defn merger
  "Packages up a Clojure fn in a kstream Merger"
  ^Merger [merger-fn]
  (FnMerger. merger-fn))

;;
;; Wrappers that deal with Avro schema serde concerns for us.
;;

(defn avro-initializer
  [agg-schema initializer-fn]
  (initializer
   (fn []
     (let [init-agg (initializer-fn)]
       (log/info "in initializer" {:init-agg init-agg}) ; FIXME: del eventually
       (avro/->java agg-schema init-agg)))))

(defn avro-aggregator
  [agg-schema aggregator-fn]
  (aggregator
   (fn [agg [k v]]
     (let [k (and k (avro/->clj k))
           v (and v (avro/->clj v))
           agg (and agg (avro/->clj agg))
           new-agg (aggregator-fn agg [k v])]
       (log/info "in aggregator" {:k k :v v :agg agg :new-agg new-agg}) ; FIXME: del eventually
       (avro/->java agg-schema new-agg)))))

(defn avro-merger
  [agg-schema merger-fn]
  (merger
   (fn [k agg1 agg2]
     (let [k (and k (avro/->clj k))
           agg1 (and agg1 (avro/->clj agg1))
           agg2 (and agg2 (avro/->clj agg2))
           new-agg (merger-fn k agg1 agg2)]
       (log/info "in merger" {:k k :agg1 agg1 :agg2 agg2 :new-agg new-agg}) ; FIXME: del eventually
       (avro/->java agg-schema new-agg)))))

(defn time-windows
  ;; Hopping (overlapping) windows, where advance-by-duration is less than window-duration.
  ([window-duration advance-by-duration]
   (let [windows (TimeWindows/of window-duration)]
     (if advance-by-duration
       (.advanceBy windows advance-by-duration)
       windows)))
  ;; Tumbling (non-overlapping, gap-less) windows, where advance-by-duration is the same as window-duration.
  ([window-duration]
   (time-windows window-duration nil)))

(defn session-windows
  [inactivity-duration]
  (SessionWindows/with inactivity-duration))

;; Returns a value for passing to KTable#suppress method
;; See https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/Suppressed.html#untilWindowCloses-org.apache.kafka.streams.kstream.Suppressed.StrictBufferConfig-
;; https://cwiki.apache.org/confluence/display/KAFKA/KIP-328%3A+Ability+to+suppress+updates+for+KTables
;; FIXME: When "spill to disk" buffer config is available, use that instead of "unbounded".
;; https://issues.apache.org/jira/browse/KAFKA-7224
(defn suppressed-until-window-closes
  []
  (Suppressed/untilWindowCloses
   (Suppressed$BufferConfig/unbounded)))

;; Returns a value to pass as explicit Materialized arg to stateful transformations (like aggregate). Seems to be necessary (at least with kafka-streams 2.1.0) when using `suppress`.
;; See https://stackoverflow.com/a/54115693/11023580
(defn materialized-with-avro
  [schema-registry-url]
  (let [config {AbstractKafkaAvroSerDeConfig/SCHEMA_REGISTRY_URL_CONFIG schema-registry-url}]
    (Materialized/with (doto (GenericPrimitiveAvroSerde.)
                         (.configure config true))
                       (doto (GenericPrimitiveAvroSerde.)
                         (.configure config false)))))


(defn- str-array
  "Helper for passing a vararg of non-nil Strings to a Java method."
  [& strings]
  (into-array String (filter identity strings)))

(defn transform
  [kstream component transformer-key]
  (.transform kstream
              (transformer/transformer-supplier component transformer-key)
              (str-array (get-in component [:transformers transformer-key :store-name]))))

(defn- add-state-stores
  [{:keys [transformers] :as component} builder]
  (let [store-names (keep :store-name (vals transformers))]
    (doseq [store-name store-names]
      (.addStateStore builder (store/state-store-builder store-name)))
    builder))

(defrecord KafkaStreamDslProcessor [bootstrap-servers schema-registry-url app-id input-topics process-input-stream auto-register-schemas?]
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
          builder (add-state-stores this (StreamsBuilder.))
          input-stream (.stream builder input-topics)
          ;; The following mutates the builder as a side-effect.
          _ (process-input-stream this input-stream)
          topology (.build builder)
          streams (KafkaStreams. topology config)]
      (.start streams)
      (assoc this :streams streams)))
  (stop [this]
    (try
      (.close (:streams this))
      (catch Throwable t
        (common/report-throwable this t "Caught exception closing streams" {:app-id app-id})))
    this))
