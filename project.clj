(defproject com.rentpath/rp-kafka-clj "0.0.4-SNAPSHOT"
  :description "Generic Clojure Kafka components and utils"
  :url "https://gitthub.com/rentpath/rp-kafka-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.stuartsierra/component "0.3.2"]
                 [cheshire "5.8.0"]
                 [org.apache.kafka/kafka-streams "2.1.0"]
                 [io.confluent/kafka-streams-avro-serde "5.1.0"]
                 [org.apache.avro/avro "1.8.2"]]
  :repositories [["confluent" {:url "https://packages.confluent.io/maven/"}]]
  :scm {:url "git@github.com:rentpath/rp-kafka-clj.git"}
  :deploy-repositories [["releases" {:url "https://clojars.org/repo/"
                                     :username [:gpg :env/CLOJARS_USERNAME]
                                     :password [:gpg :env/CLOJARS_PASSWORD]
                                     :sign-releases false}]]
  :java-source-paths ["src/java"])
