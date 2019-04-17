(ns rp.kafka.kafka-subscriber
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [rp.kafka.avro :as avro]
            [rp.kafka.common :as common])
  (:import [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig ConsumerRecords]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors SerializationException]
           [io.confluent.kafka.serializers AbstractKafkaAvroSerDeConfig KafkaAvroDeserializer]
           [java.util Properties]))

(defn seek-past-error
  [consumer e]
  ;; Annoyingly the exception doesn't provide the details as data so we have to parse the error message.
  ;; Example message: "Error deserializing key/value for partition foo-0 at offset 2. If needed, please seek past the record to continue consumption."
  (let [msg (.getMessage e)
        match (re-find #"partition (.+)-(\d+) at offset (\d+)" msg)]
    (if match
      (let [[_ topic partition offset] match
            partition (Long/parseLong partition)
            offset (Long/parseLong offset)]
        (log/info "Trying to seek past bad record" {:topic topic :partition partition :offset offset})
        (.seek consumer (TopicPartition. topic partition) (inc offset)))
      (throw (ex-info "Failed to parse serialization error; can't seek past bad message." {:msg msg})))))

(defn fetch-records
  [{:keys [consumer poll-timeout-ms] :as component}]
  (try (vec (.poll consumer poll-timeout-ms))
       (catch SerializationException e
         (common/report-throwable component e "Caught SerializationException calling .poll")
         (seek-past-error consumer e)
         [])))

(defn commit-offsets
  [{:keys [consumer] :as component}]
  (try
    (.commitSync consumer)
    (catch Throwable t
      (common/report-throwable component t "Caught exception committing offsets"))))

(defn poll
  [{:keys [consumer record-callback] :as component}]
  (let [recs (fetch-records component)]
    (doseq [rec recs]
      (log/info {:rec rec}) ; FIXME: del eventually
      (try
        (let [k (.key rec)
              v (avro/->clj (.value rec))
              meta {:topic (.topic rec)
                    :partition (.partition rec)
                    :offset (.offset rec)
                    :timestamp (.timestamp rec)}]
          ;; FIXME: switch to this style after initial alert-service migration
          #_(record-callback {:component component
                            :k k
                            :v v
                            :meta meta})
          (record-callback component k v meta))
        (catch Throwable t
          (common/report-throwable component t "Caught exception handling record" {:rec rec}))))
    (when (seq recs)
      (commit-offsets component))))

(defrecord KafkaSubscriber [bootstrap-servers group-id topics schema-registry-url poll-timeout-ms record-callback]
  component/Lifecycle
  (start [this]
    (let [config {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                  ConsumerConfig/GROUP_ID_CONFIG group-id
                  ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG KafkaAvroDeserializer
                  ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG KafkaAvroDeserializer
                  AbstractKafkaAvroSerDeConfig/SCHEMA_REGISTRY_URL_CONFIG schema-registry-url
                  ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                  ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG false}
          consumer (KafkaConsumer. config)
          component (assoc this
                           :consumer consumer
                           :running? (atom true))]
      (assoc component :poll-loop-future
             (future (try
                       (.subscribe consumer topics)
                       (while @(:running? component) (poll component))
                       (catch Throwable t
                         (common/report-throwable component t "Caught exception running poll loop")))
                     (log/info "Poll loop stopped running for" topics)
                     (try
                       (.close consumer)
                       (catch Throwable t
                         (common/report-throwable component t "Caught exception closing consumer")))))))
  (stop [{:keys [running?] :as this}]
    (reset! running? false)
    this))

(comment
  (defn callback
    ;; FIXME: update to expect single map arg...
    [comp k v meta]
    (log/info "demo callback" {:comp comp :k k :v v :meta meta}))

  (def sub (map->KafkaSubscriber
            {:bootstrap-servers "kafka:9092"
             :group-id "test-subscriber"
             :topics ["foo" "bar"]
             :poll-timeout-ms 1000
             :schema-registry-url "http://schema_registry:8081"
             :record-callback callback}))
  (def sub (component/start sub))
  (def sub (component/stop sub))
  )
