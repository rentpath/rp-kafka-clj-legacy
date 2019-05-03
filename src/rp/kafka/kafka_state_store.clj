(ns rp.kafka.kafka-state-store
  "Adds a protocol-based wrapper for interacting with KV state stores.
  Opens the door for mocking.
  Includes some store-related utilities as well."
  (:require [cheshire.core :as json])
  (:import [org.apache.kafka.streams.state KeyValueStore Stores]
           [org.apache.kafka.common.serialization Serdes]))

(defprotocol KVStore
  (get-key [this k] "Gets the value for key")
  (set-key [this k v] "Sets the value for key; deletes key when value is nil."))

(extend-type KeyValueStore
  KVStore
  (get-key [this k]
    (.get this (name k)))
  (set-key [this k v]
    (.put this (name k) v)))

(defrecord MockKVStore [store]
  KVStore
  (get-key [this k]
    (get @store (keyword k)))
  (set-key [this k v]
    (if v
      (swap! store assoc (keyword k) v)
      (swap! store dissoc (keyword k)))
    nil))

(defn state-store-builder
  "Returns a builder (for use with `.addStateStore`) for a persistent store with the specified name and serdes. Defaults to String serdes."
  ([store-name key-serde value-serde]
   (Stores/keyValueStoreBuilder
    (Stores/persistentKeyValueStore store-name)
    key-serde
    value-serde))
  ([store-name]
   (state-store-builder store-name (Serdes/String) (Serdes/String))))

;;
;; A couple of convenience functions for getting/setting JSON string values.
;; These assume that the store was configured to use String serdes.
;;

(defn get-json
  [kvstore k]
  (let [v (get-key kvstore k)]
    (and v (json/decode v true))))

(defn set-json
  [kvstore k v]
  (set-key kvstore k (and v (json/encode v))))

;;
;; Misc
;;

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
