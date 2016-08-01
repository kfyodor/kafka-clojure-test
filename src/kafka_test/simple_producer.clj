(ns kafka-test.simple-producer
  (:require [kafka-test
             [util :as u]
             [avro :as avro]]
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
           [io.confluent.kafka.serializers
            KafkaAvroSerializer]
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
                    :schema.registry.url "http://localhost:8081"
                    :acks "all"
                    :retries 0
                    :batch.size 16384
                    :linger.ms 1
                    :buffer.memory 33554432
                    :key.serializer "org.apache.kafka.common.serialization.StringSerializer"
                    :value.serializer "io.confluent.kafka.serializers.KafkaAvroSerializer"})

(defn- make-producer-record
  ([{:keys [topic
            key
            value
            partition
            timestamp]
     :as record}]
   {:pre [(and (not (nil? value)) (not (nil? topic)))]}
   (ProducerRecord. topic partition timestamp key value)))

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
  ([^Producer producer record]
   (send! producer record nil))
  ([^Producer producer record callback]
   (let [record   (make-producer-record record)
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
    (let [producer (kafka-producer :props default-props)
          ch (:ch source)]
      (go
        (loop []
          (let [msg (avro/make-message :user (<! ch))]
            (send! producer msg (fn [_ ex]
                                  (when ex
                                    (log/error ex))))
            (recur))))
      this))
  (stop [this]  this))

(defn make-simple-producer [config]
  (map->SimpleProducer {:config config}))
