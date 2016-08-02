(ns kafka-test.consumer
  (:require [kafka-test.util :as u]
            [schema.core :as s]
            [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer [thread go timeout <!]]
            [kafka-test.avro :as avro])
  (:import  [org.apache.kafka.clients.consumer
             KafkaConsumer
             ConsumerRecord
             ConsumerRecords
             OffsetAndMetadata]
            [io.confluent.kafka.serializers
             KafkaAvroDeserializer]
            [org.apache.kafka.common
             TopicPartition]
            [org.apache.kafka.common.serialization Deserializer StringDeserializer]))

(s/defschema ConsumerArgs
  {(s/optional-key :key-deserializer)   Deserializer
   (s/optional-key :value-deserializer) Deserializer
   :props u/PropsMap})

(s/defn kafka-consumer :- KafkaConsumer
  [& args :- [ConsumerArgs]]
  (let [{:keys [key-deserializer value-deserializer props]
         :or {key-deserializer nil
              value-deserializer nil}} args
        props (u/make-props props)]
    (KafkaConsumer. props key-deserializer value-deserializer)))

(defn subscribe! [consumer topics]
  (doto consumer
    (.subscribe topics)))

(defn- map->TopicPartition [{:keys [partition topic]}]
  (TopicPartition. topic partition))

(defn- TopicPartition->map [^TopicPartition tp]
  {:topic (.topic tp)
   :partition (.partition tp)})


(defn ConsumerRecord->map
  [^ConsumerRecord record]
  {:key       (.key record)
   :offset    (.offset record)
   :partition (.partition record)
   :timestamp (.timestamp record)
   :topic     (.topic record)
   :value     (.value record)})

(defn poll! [consumer poll-timeout]
  (->> (iterator-seq (.iterator (.poll consumer poll-timeout)))
       (map ConsumerRecord->map)))

(defn stream
  ([consumer] (stream consumer {}))
  ([consumer {:keys [poll-timeout commit-prev] :or {poll-timeout 3000 commit-prev false} :as opts}]
   (when commit-prev (.commitSync consumer))
   (lazy-cat (poll! consumer poll-timeout)
             (lazy-seq (stream consumer
                               (assoc opts :commit-prev true))))))

(defn calculate-stats
  [{:keys [mps msg-cnt] :as stats}]
  (if (zero? mps)
    (assoc stats
           :mps msg-cnt
           :msg-cnt 0)
    (assoc stats
           :mps (float (/ (+ msg-cnt mps) 2))
           :msg-cnt 0)))

(defrecord Consumer [config conn]
  component/Lifecycle

  (start [this]
    (let [consumer (-> (kafka-consumer :props {:enable.auto.commit false
                                               :schema.registry.url "http://localhost:8081"
                                               :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
                                               :value.deserializer "io.confluent.kafka.serializers.KafkaAvroDeserializer"
                                               :group.id "test"
                                               :bootstrap.servers [(config :kafka-bootstrap-server)]})
                       (subscribe! ["user"]))]
      (go
        (thread
          (try
            (doseq [record (avro/parse-avro-stream
                            (stream consumer {:poll-timeout 100}))]
              (log/info record))

            (catch Exception e
              (log/error e "Error in consumer"))))))
    this)
  (stop  [this]
    this))

(defn make-consumer [config]
  (map->Consumer {:config config}))
