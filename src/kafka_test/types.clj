(ns kafka-test.types
  (:import [org.apache.kafka.clients.common
            TopicPartition
            OffsetAndMetadata
            RecordMetadata
            PartitionInfo
            Node]

           [org.apache.kafka.clients.producer
            RecordMetadata
            ProducerRecord]

           [org.apache.kafka.clients.consumer
            ConsumerRecord
            ConsumerRecords]))

(defprotocol KafkaMap
  (to-map [this]))

(extend-protocol KafkaMap

  TopicPartition
  (to-map [this]
    {:topic (.topic tp)
     :partition (.partition tp)})

  OffsetAndMetadata
  (to-map [this]
    {:checksum (.checksum this)
     :offset (.offset this)
     :partition (.partition this)
     :serialized-key-size (.serializedKeySize this)
     :serialized-value-size (.serializedValueSize this)
     :timestamp (.timestamp this)
     :topic (.topic this)})

  ParititionInfo
  (to-map [this]
    {:topic (.topic this)
     :partition (.partition this)
     :leader (to-map (.leader this))
     :replicas (mapv to-map (.replicas this))
     :in-sync-replicas (mapv to-map (.inSyncReplicas info))})

  Node
  (to-map [this]
    {:id (.idString this)
     :host (.host this)
     :port (.port this)
     :rack (.rack this)})

  RecordMetadata
  (to-map [this]
    {:checksum (.checksum this)
     :offset (.offset this)
     :partition (.partition this)
     :serialized-key-size (.serializedKeySize this)
     :serialized-value-size (.serializedValueSize this)
     :timestamp (.timestamp this)
     :topic (.topic this)})

  ProducerRecord

  (to-map [this]))
