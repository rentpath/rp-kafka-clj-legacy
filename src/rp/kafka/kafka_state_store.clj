(ns rp.kafka.kafka-state-store
  "Adds a protocol-based wrapper for interacting with KV state stores.
  Opens the door for mocking."
  (:require [cheshire.core :as json])
  (:import [org.apache.kafka.streams.state KeyValueStore]))

(defprotocol KVStore
  (get-key [this k] "Gets the value for key")
  (set-key [this k v] "Sets the value for key; deletes key when value is nil."))

(extend-type KeyValueStore
  KVStore
  (get-key [this k]
    (.get this (name k)))
  (set-key [this k v]
    (.put this (name k) v)))

;; A couple of convenience functions for dealing with values as json strings:

(defn get-json
  [kvstore k]
  (let [v (get-key kvstore k)]
    (and v (json/decode v true))))

(defn set-json
  [kvstore k v]
  (set-key kvstore k (and v (json/encode v))))

(defrecord MockKVStore [store]
  KVStore
  (get-key [this k]
    (get @store (keyword k)))
  (set-key [this k v]
    (if v
      (swap! store assoc (keyword k) v)
      (swap! store dissoc (keyword k)))
    nil))

(defn- print-kv-iter
  "Helper for print-state-store"
  [iter]
  (when (.hasNext iter)
    (let [kv (.next iter)]
      (prn kv))
    (print-kv-iter iter)))

(defn print-state-store
  "Print all the keys/values in a KeyValueStore. Handy for dev debugging."
  [state-store]
  (println "=== Start printing state store ===")
  (let [iter (.all state-store)]
    (print-kv-iter iter)
    (.close iter))
    (println "=== Done printing state store ==="))
