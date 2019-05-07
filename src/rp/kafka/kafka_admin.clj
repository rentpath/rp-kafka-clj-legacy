(ns rp.kafka.kafka-admin
  (:import [java.util Map]
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic TopicDescription]))

(defn ->AdminClient
  "Convenient factory for constructing an AdminClient.
  The caller is responsible for calling `.close` on the client when its no longer needed."
  ^AdminClient
  [bootstrap-servers]
  (let [config {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers}]
    (AdminClient/create ^Map config)))

(defn map->NewTopic
  "Creates a NewTopic from a config map. Minimum config is just :name."
  [{:keys [name partition-count replication-factor] :as m}]
  (NewTopic. ^String name
             (or partition-count 1)
             (or replication-factor 1)))

(defn list-topics
  [^AdminClient client]
  (-> client .listTopics .names deref sort))

;; Does not block until the created topics are ready.
;; It may take some time for replicas and leaders to be chosen for newly created topics.
(defn create-topics!
  [^AdminClient client topic-configs]
  (->> (.createTopics client (map map->NewTopic topic-configs))
       .all deref))

;; Does not block until the topics are deleted, just until the deletion
;; requests are acknowledged.
;; Throws an exception if any of the topics don't exist.
(defn delete-topics!
  [^AdminClient client topic-names]
  (-> (.deleteTopics client topic-names)
      .all deref))

(defn re-delete-topics!
  "Delete topics matching a regex."
  [client re]
  (let [topics-to-delete (filter #(re-find re %)
                                 (list-topics client))]
    (delete-topics! client topics-to-delete)))

;; FIXME: For general re-use, it would be nice for this to return a data map for each topic.
;; jackdaw does that by providing datafy implementations for all of the java objects.
;; However right now all I really care about is partition count.
(defn- describe-topics
  [^AdminClient client topic-names]
  (-> (.describeTopics client topic-names)
      .all
      deref))

(defn- describe-topic
  ^TopicDescription
  [client topic-name]
  (get (describe-topics client [topic-name]) topic-name))

(defn partition-count
  [client topic-name]
  (-> (describe-topic client topic-name)
      .partitions
      count))
