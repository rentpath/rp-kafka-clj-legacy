(ns rp.kafka.kafka-stream-processor
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
           [org.apache.kafka.streams.kstream KStream KTable Printed Initializer Aggregator Merger Windows TimeWindows SessionWindows KeyValueMapper Suppressed Suppressed$BufferConfig Materialized Predicate KGroupedStream KGroupedTable SessionWindowedKStream TimeWindowedKStream]
           GenericPrimitiveAvroSerde
           [io.confluent.kafka.serializers AbstractKafkaAvroSerDeConfig KafkaAvroDeserializer]
           [java.time Duration]
           [java.util Collection Properties]))

(defn- str-array
  "Helper for passing a vararg of Strings to a Java method."
  [strings]
  (into-array String strings))

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
  ^TimeWindows
  ;; Hopping (overlapping) windows, where advance-by-duration is less than window-duration.
  ([^Duration window-duration ^Duration advance-by-duration]
   (let [windows (TimeWindows/of window-duration)]
     (if advance-by-duration
       (.advanceBy windows advance-by-duration)
       windows)))
  ;; Tumbling (non-overlapping, gap-less) windows, where advance-by-duration is the same as window-duration.
  ([window-duration]
   (time-windows window-duration nil)))

(defn session-windows
  ^SessionWindows
  [^Duration inactivity-duration]
  (SessionWindows/with inactivity-duration))

(defmulti grace
  (fn [windows duration]
    (class windows)))

(defmethod grace TimeWindows
  ^TimeWindows
  [^TimeWindows windows ^Duration duration]
  (.grace windows duration))

(defmethod grace SessionWindows
  ^SessionWindows
  [^SessionWindows windows ^Duration duration]
  (.grace windows duration))

;; Returns a value for passing to KTable#suppress method
;; See https://kafka.apache.org/21/javadoc/org/apache/kafka/streams/kstream/Suppressed.html#untilWindowCloses-org.apache.kafka.streams.kstream.Suppressed.StrictBufferConfig-
;; https://cwiki.apache.org/confluence/display/KAFKA/KIP-328%3A+Ability+to+suppress+updates+for+KTables
;; FIXME: When "spill to disk" buffer config is available, use that instead of "unbounded".
;; https://issues.apache.org/jira/browse/KAFKA-7224
(defn suppressed-until-window-closes
  ^Suppressed
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

;;
;; Wrappers for Streams API methods
;; (only the subset we've needed so far; add more as we necessary)
;; These are intended for use inside your component's process-input-stream fn.
;;

(defn to
  [^KStream kstream ^String topic-name]
  (.to kstream topic-name))

(defn to-stream
  ^KStream
  [^KTable ktable]
  (.toStream ktable))

;; Caution: This name shadows clojure.core/map in this NS
(defn map
  ^KStream
  [^KStream kstream key-value-mapper-fn]
  (.map kstream (key-value-mapper key-value-mapper-fn)))

;; Caution: This name shadows clojure.core/filter in this NS
(defn filter
  ^KStream
  [^KStream kstream pred-fn]
  (.filter kstream (predicate pred-fn)))

(defn filter-not
  ^KStream
  [^KStream kstream pred-fn]
  (.filterNot kstream (predicate pred-fn)))

(defn group-by-key
  ^KGroupedStream
  [^KStream kstream]
  (.groupByKey kstream))

(defmulti windowed-by
  (fn [kgrouped-stream windows]
    (class windows)))

(defmethod windowed-by TimeWindows
  ^TimeWindowedKStream
  [^KGroupedStream kgrouped-stream ^TimeWindows windows]
  (.windowedBy kgrouped-stream windows))

(defmethod windowed-by SessionWindows
  ^SessionWindowedKStream
  [^KGroupedStream kgrouped-stream ^SessionWindows windows]
  (.windowedBy kgrouped-stream windows))

;; Note: This one's interesting as it can be called on a few different classes and has multiple arities.
(defmulti aggregate
  (fn [target & more]
    [(class target) (count more)]))

(defmethod aggregate [KGroupedStream 3]
  ^KTable
  [^KGroupedStream target ^Initializer initializer ^Aggregator aggregator ^Materialized materialized]
  (.aggregate target initializer aggregator materialized))

(defmethod aggregate [KGroupedStream 2]
  ^KTable
  [^KGroupedStream target ^Initializer initializer ^Aggregator aggregator]
  (.aggregate target initializer aggregator))

(defmethod aggregate [SessionWindowedKStream 4]
  ^KTable
  [^SessionWindowedKStream target ^Initializer initializer ^Aggregator aggregator ^Merger merger ^Materialized materialized]
  (.aggregate target initializer aggregator merger materialized))

(defmethod aggregate [SessionWindowedKStream 3]
  ^KTable
  [^SessionWindowedKStream target ^Initializer initializer ^Aggregator aggregator ^Merger merger]
  (.aggregate target initializer aggregator merger))

(defmethod aggregate [TimeWindowedKStream 3]
  ^KTable
  [^TimeWindowedKStream target ^Initializer initializer ^Aggregator aggregator ^Materialized materialized]
  (.aggregate target initializer aggregator materialized))

(defmethod aggregate [TimeWindowedKStream 2]
  ^KTable
  [^TimeWindowedKStream target ^Initializer initializer ^Aggregator aggregator]
  (.aggregate target initializer aggregator))

(defmethod aggregate [KGroupedTable 4]
  ^KTable
  [^KGroupedTable target ^Initializer initializer ^Aggregator adder ^Aggregator subtractor ^Materialized materialized]
  (.aggregate target initializer adder subtractor materialized))

(defmethod aggregate [KGroupedTable 3]
  ^KTable
  [^KGroupedTable target ^Initializer initializer ^Aggregator adder ^Aggregator subtractor]
  (.aggregate target initializer adder subtractor))

(defn suppress
  ^KTable
  [^KTable ktable ^Suppressed suppressed]
  (.suppress ktable suppressed))

(defn transform
  ^KStream
  [kstream transformer-supplier store-names]
  (.transform ^KStream kstream
              ^TransformerSupplier transformer-supplier
              (str-array store-names)))

;;
;; End of Streams API method wrappers
;;

;; This function is intended to be used in a process-input-stream call,
;; but instead of wrapping a Streams DSL method (`.transform`) directly
;; it relies on the idea of a :transformers configuration map at the component-level.
;; See rp.kafka.kafka-transformer for more detail.
(defn run-transformer
  ^KStream
  [kstream component transformer-key]
  (let [store-name (get-in component [:transformers transformer-key :store-name])]
    (transform kstream
               (transformer/transformer-supplier component transformer-key)
               (and store-name [store-name]))))

(defn- add-state-stores
  [{:keys [transformers] :as component} ^StreamsBuilder builder]
  (let [store-names (keep :store-name (vals transformers))]
    (doseq [store-name store-names]
      (.addStateStore builder (store/state-store-builder store-name)))
    builder))

(defrecord KafkaStreamProcessor [bootstrap-servers schema-registry-url app-id input-topics process-input-stream auto-register-schemas?]
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
          builder ^StreamsBuilder (add-state-stores this (StreamsBuilder.))
          input-stream (.stream builder ^Collection input-topics)
          ;; The following mutates the builder as a side-effect.
          _ (process-input-stream this input-stream)
          topology (.build builder)
          streams (KafkaStreams. topology config)]
      (.start streams)
      (assoc this :streams streams)))
  (stop [this]
    (try
      (.close ^KafkaStreams (:streams this))
      (catch Throwable t
        (common/report-throwable this t "Caught exception closing streams" {:app-id app-id})))
    this))
