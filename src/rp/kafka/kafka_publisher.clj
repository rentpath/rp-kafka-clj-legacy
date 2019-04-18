(ns rp.kafka.kafka-publisher
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [rp.kafka.publisher :as publisher]
            [rp.kafka.avro :as avro]
            [rp.kafka.common :as common])
  (:import [org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord]
           [io.confluent.kafka.serializers AbstractKafkaAvroSerDeConfig KafkaAvroSerializer]
           [java.util Properties]))

(defn report-success [component k v metadata]
  ;; Call success-callback (when specified)
  (when-let [callback (:success-callback component)]
    (callback component k v metadata)))

;; A helper for multiple artity protocol method.
(defn- publish*
  [{:keys [topic key-schema value-schema producer] :as component} k v {:keys [partition] :as opts}]
  (.send producer
         (let [k-payload (and k (avro/->java key-schema k))
               v-payload (and v (avro/->java value-schema v))]
           (if partition
             (ProducerRecord. topic (int partition) k-payload v-payload)
             (ProducerRecord. topic k-payload v-payload)))
         (reify Callback
           (onCompletion [_ metadata e]
             (if e
               (common/report-throwable component e "Failed to send record to Kafka" {:k k :v v :opts opts})
               (report-success component k v metadata))))))

(defrecord KafkaPublisher [bootstrap-servers topic schema-registry-url key-schema value-schema auto-register-schemas?]
  component/Lifecycle
  (start [this]
    (let [config {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                  ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG KafkaAvroSerializer
                  ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG KafkaAvroSerializer
                  AbstractKafkaAvroSerDeConfig/SCHEMA_REGISTRY_URL_CONFIG schema-registry-url
                  AbstractKafkaAvroSerDeConfig/AUTO_REGISTER_SCHEMAS auto-register-schemas?}
          producer (KafkaProducer. config)]
      (assoc this :producer producer)))
  (stop [{:keys [producer] :as this}]
    (when producer
      (.flush producer)
      (.close producer))
    (dissoc this :producer))

  publisher/Publisher
  (publish [this k v opts]
    (publish* this k v opts))
  (publish [this k v]
    (publish* this k v nil)))

(comment
  (require '[cheshire.core :as json])
  (def value-schema (-> {:type "record"
                         :name "Test"
                         :fields [{:name "x" :type "string"}]}
                        json/encode
                        avro/parse-schema))
  (def key-schema (avro/parse-schema "string"))
  (def pub (map->KafkaPublisher
            {:bootstrap-servers "kafka:9092"
             :topic "foo"
             :key-schema key-schema
             :value-schema value-schema
             :schema-registry-url "http://schema_registry:8081"
             :auto-register-schemas? true}))
  (def pub (component/start pub))
  (publisher/publish pub
                     "test"
                     {:x "Hi"})
  (publisher/publish pub
                     "test"
                     ;; V = nil indicates deletion
                     nil)
  (def pub (component/stop pub))
  )
