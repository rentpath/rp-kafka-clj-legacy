;; This is a modified verion of https://github.com/konukhov/kfk-avro-bridge/blob/master/src/thdr/kfk/avro_bridge/core.clj
;; Removed the key munging (`camel-snake-kebab` stuff) which is too aggressive
;; for our needs (for example, turning `addressline1` into
;; `addressline_1` in `->java` and `addressline-1` in `->clj`).
;; Then added limited munging in `->java` that just replaces `-` with `_` in keys.
;; Removed Clojure 1.9 shims (so this requires 1.9)
(ns rp.kafka.avro
  (:require [clojure.string :as str])
  (:import [java.nio ByteBuffer]
           [java.util Collection]
           [org.apache.avro.generic
            GenericData
            GenericData$Record
            GenericData$Array
            GenericData$Fixed
            GenericData$EnumSymbol]
           [org.apache.avro.util Utf8]
           [org.apache.avro Schema Schema$Type Schema$Field]))

(defn avro-friendly-name
  [k]
  (str/replace (name k) "-" "_"))

(defn- throw-invalid-type
  [^Schema schema obj]
  (throw (Exception. (format "Value `%s` cannot be cast to `%s` schema"
                             (str obj)
                             (.toString schema)))))

(defn parse-schema
  "A little helper for parsing schemas"
  [^String json]
  (Schema/parse json))

(defn ->java
  "Converts a Clojure data structure to an Avro-compatible
   Java object. Avro `Schema` must be provided.
   Hyphens in keys will be replaced with underscores, since hyphens are not
   valid in Avro names."
  [schema obj]
  (condp = (and (instance? Schema schema) (.getType ^Schema schema))
    Schema$Type/NULL
    (if (nil? obj)
      nil
      (throw-invalid-type schema obj))

    Schema$Type/INT
    (if (and (integer? obj) (<= Integer/MIN_VALUE obj Integer/MAX_VALUE))
      (int obj)
      (throw-invalid-type schema obj))

    Schema$Type/LONG
    (if (and (integer? obj) (<= Long/MIN_VALUE obj Long/MAX_VALUE))
      (long obj)
      (throw-invalid-type schema obj))

    Schema$Type/FLOAT
    (if (float? obj)
      (float obj)
      (throw-invalid-type schema obj))

    Schema$Type/DOUBLE
    (if (float? obj)
      (double obj)
      (throw-invalid-type schema obj))

    Schema$Type/BOOLEAN
    (if (boolean? obj)
      obj
      (throw-invalid-type schema obj))

    Schema$Type/STRING
    (if (string? obj)
      obj
      (throw-invalid-type schema obj))

    Schema$Type/BYTES
    (if (bytes? obj)
      (doto (ByteBuffer/allocate (count obj))
        (.put (bytes obj))
        (.position 0))
      (throw-invalid-type schema obj))

    Schema$Type/ARRAY ;; TODO Exception for complex type
    (let [f (partial ->java (.getElementType ^Schema schema))]
      (GenericData$Array. ^Schema schema ^Collection (map f obj)))

    Schema$Type/FIXED
    (if (and (bytes? obj) (= (count obj) (.getFixedSize ^Schema schema)))
      (GenericData$Fixed. schema obj)
      (throw-invalid-type schema obj))

    Schema$Type/ENUM
    (let [enum (name obj)
          enums (into #{} (.getEnumSymbols ^Schema schema))]
      (if (contains? enums enum)
        (GenericData$EnumSymbol. ^Schema schema ^String enum)
        (throw-invalid-type schema enum)))

    Schema$Type/MAP ;; TODO Exception for complex type
    (zipmap (map avro-friendly-name (keys obj))
            (map (partial ->java (.getValueType ^Schema schema))
                 (vals obj)))

    Schema$Type/UNION
    (let [[val matched]
          (reduce (fn [_ schema]
                    (try
                      (reduced [(->java schema obj) true])
                      (catch Exception _
                        [nil false])))
                  [nil false]
                  (.getTypes ^Schema schema))]
      (if matched
        val
        (throw-invalid-type schema obj)))

    Schema$Type/RECORD
    (reduce-kv
     (fn [record k v]
       (let [k ^String (avro-friendly-name k)
             s ^Schema (some-> (.getField ^Schema schema k)
                       (as-> f (.schema ^Schema$Field f)))]
         (doto ^GenericData$Record record
           (.put ^String k ^Object (->java (or s k) v)))))
     (GenericData$Record. schema)
     obj)

    (throw (Exception. (format "Field `%s` is not in schema" schema)))))

(defn ->clj
  "Parses deserialized Avro object into
   a Clojure data structure. Keys in records
   and maps + enums will get keywordized."
  [msg]
  (condp instance? msg
    Utf8
    (str msg)

    java.nio.ByteBuffer
    (.array ^ByteBuffer msg)

    GenericData$Array
    (into [] (map ->clj msg))

    GenericData$Fixed
    (.bytes ^GenericData$Fixed msg)

    GenericData$EnumSymbol
    (keyword (str msg))

    java.util.Map
    (zipmap (map (comp keyword str) (keys msg))
            (map ->clj (vals msg)))

    GenericData$Record
    (loop [fields (seq (.. ^GenericData$Record msg getSchema getFields)) record {}]
      (if-let [f (first fields)]
        (let [n (.name ^Schema$Field f)
              v (->clj (.get ^GenericData$Record msg n))
              k (keyword n)]
          (recur (rest fields)
                 (assoc record k v)))
        record))

    msg))
