(ns rp.kafka.avro-test
  (:require [clojure.test :refer :all]
            [rp.kafka.avro :as sut]
            [cheshire.core :as json]))

(def schema-clj {:namespace "com.test"
                 :type "record"
                 :name "Test"
                 :fields [{:name "a_string" :type "string"}
                          {:name "an_int" :type "int"}
                          {:name "a_boolean" :type "boolean"}
                          {:name "an_array" :type {:type "array" :items "int"}}
                          {:name "an_enum" :type {:type "enum" :name "EnumTest" :symbols ["foo" "bar"]}}]})

(def schema (sut/parse-schema (json/encode schema-clj)))

(deftest round-trip-test
  (let [payload {:a_string "Hello"
                 :an_int 23
                 :a_boolean true
                 :an_array [1 2 3]
                 :an_enum :foo}]
    (testing "snake_case passes through"
      (is (= payload
             (->> payload
                  (sut/->java schema)
                  sut/->clj))))
    (testing "translates kebab-case to snake_case"
      (is (= payload
             (->> {:a-string "Hello"
                   :an-int 23
                   :a-boolean true
                   :an-array [1 2 3]
                   :an-enum :foo}
                  (sut/->java schema)
                  sut/->clj))))))
