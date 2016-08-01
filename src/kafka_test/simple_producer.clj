(ns kafka-test.simple-producer
  (:require [kafka-test.util :as u]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [clojure.core.async :refer [go <!]])
  (:import [org.apache.kafka.clients.producer
            KafkaProducer
            Producer
            ProducerRecord
            Callback
            RecordMetadata]
           [org.apache.kafka.common
            PartitionInfo
            Node]
           [org.apache.kafka.common.serialization Serializer StringSerializer]))

(s/defschema ProducerArgs
  {(s/optional-key :key-serializer) Serializer
   (s/optional-key :value-serializer) Serializer
   :props u/PropsMap})

(defn kafka-producer
  [& {:keys [^Serializer key-serializer
             ^Serializer value-serializer
             props]
      :or {key-serializer nil
           value-serializer nil}
      :as args}]
  {:pre [(s/validate ProducerArgs args)]}
  (-> (u/make-props props)
      (KafkaProducer. key-serializer value-serializer)))

(def default-props {:bootstrap.servers ["localhost:9092"]
                    :acks "all"
                    :retries 0
                    :batch.size 16384
                    :linger.ms 1
                    :buffer.memory 33554432})

(defn- map->ProducerRecord
  ([^String topic record]
   (map->ProducerRecord topic {} record))
  ([^String topic
    {:keys [key
            key-fn
            partition
            timestamp
            timestamp-fn]
     :as opts}
    record]
   (let [key (or key (and key-fn (key-fn record)))
         ts  (or timestamp (and timestamp-fn (timestamp-fn record)))]
     (ProducerRecord. topic partition ts key record))))

(defn- Node->map
  [^Node node]
  {:id (.idString node)
   :host (.host node)
   :port (.port node)
   :rack (.rack node)})

(defn- PartitionInfo->map
  [^PartitionInfo info]
  {:topic (.topic info)
   :partition (.partition info)
   :leader (-> (.leader info) Node->map)
   :replicas (->> (.replicas info) (mapv Node->map))
   :in-sync-replicas (->> (.inSyncReplicas info) (mapv Node->map))})

(defn- RecordMetadata->map
  [^RecordMetadata rm]
  {:checksum (.checksum rm)
   :offset (.offset rm)
   :partition (.partition rm)
   :serialized-key-size (.serializedKeySize rm)
   :serialized-value-size (.serializedValueSize rm)
   :timestamp (.timestamp rm)
   :topic (.topic rm)})

(defn- make-send-callback [f]
  (reify Callback
    (onCompletion [_ metadata ex]
      (-> (RecordMetadata->map metadata)
          (f ex)))))

(defn send!
  "Valid options are (all are optional):
   `:key`          - record key
   `:key-fn`       - function which extracts key from record
   `:partition`    - partition number
   `:timestamp`    - record timestamp
   `:timestamp-fn` - function which extracts timestamp from record
   `:callback`     - callback (a function of record metadata map and exception)"
  ([^Producer producer ^String topic record]
   (send! producer topic record {}))
  ([^Producer producer ^String topic record {:keys [callback] :as opts}]
   (let [opts     (dissoc opts :callback)
         record   (map->ProducerRecord topic opts record)
         callback (and callback (make-send-callback callback))]
     (.send producer record callback))))

(defn flush! [^Producer producer]
  (.flush producer))

(defn close! [^Producer producer]
  (.close producer))

(defn partitions-for
  [^Producer producer ^String topic]
  (mapv PartitionInfo->map
        (.partitionsFor producer topic)))

(defrecord SimpleProducer [config source]
  component/Lifecycle

  (start [this]
    (let [producer (kafka-producer :key-serializer (StringSerializer.)
                                   :value-serializer (StringSerializer.)
                                   :props default-props)
          ch (:ch source)]
      (go
        (loop []
          (let [msg (<! ch)]
            (send! producer
                   "kafkatest"
                   (str msg)
                   {:callback (fn [_ ex]
                                (when ex
                                  (log/error "Error in producer" ex)))})
            (recur))))
      this))
  (stop [this]  this))

(defn make-simple-producer [config]
  (map->SimpleProducer {:config config}))
