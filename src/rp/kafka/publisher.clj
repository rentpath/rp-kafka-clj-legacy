(ns rp.kafka.publisher)

(defprotocol Publisher
  (publish [this k v] [this k v opts]))

;; A mock implementation for tests that keeps a record of all publish calls in an atom.
(defrecord MockPublisher [store]
  Publisher
  (publish [this k v]
    (swap! store
           conj
           {:k k
            :v v}))
  (publish [this k v opts]
    (swap! store
           conj
           {:k k
            :v v
            :opts opts})))

;; Convenience factory fn
(defn mock-publisher
  []
  (->MockPublisher (atom [])))

;; Mock helper
(defn get-mock-store
  [publisher]
  @(:store publisher))
