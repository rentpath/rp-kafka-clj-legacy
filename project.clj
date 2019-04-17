(defproject rp-kafka-clj "0.0.0"
  :description "Generic Clojure Kafka components and utils"
  :url "https://gitthub.com/rentpath/rp-kafka-clj"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.stuartsierra/component "0.3.2"]
                 [cheshire "5.8.0"]
                 [org.apache.kafka/kafka-streams "2.1.0"]
                 [io.confluent/kafka-streams-avro-serde "5.1.0"]
                 [org.apache.avro/avro "1.8.2"]]
  :repositories [["releases" {:url "https://nexus.tools.rentpath.com/repository/maven-releases/"
                              :sign-releases false
                              :username [:gpg :env/nexus_deploy_username]
                              :password [:gpg :env/nexus_deploy_password]}]
                 ["confluent" {:url "https://packages.confluent.io/maven/"}]]
  :java-source-paths ["src/java"])
