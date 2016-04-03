(ns kafka-test.producer
  (:require [clojure.core.async :refer [<! go go-loop]]

            [franzy.clients.producer.client :as p]
            [franzy.serialization.serializers :as s]

            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.common.serialization StringSerializer]))

(defrecord Producer [config source conn]
  component/Lifecycle

  (start [this]
    (if conn
      this
      (let [ch (:ch source)
            pr (p/make-producer {:bootstrap.servers [(config :kafka-bootstrap-server)]
                                 :client.id "kafka_test_client"}
                                (StringSerializer.) ;; investigate weird error with KeywordSerializer
                                (StringSerializer.))]
        (log/info "Started producer")
        (go-loop []
          (when-let [msg (<! ch)]
            (do (.send-async! pr {:topic "kafkatest"
                                  :value (str msg)})
                (recur))))
        (assoc this :conn pr))))

  (stop [this]
    (if conn
      (do (.flush! conn)
          (.close conn)
          (log/info "Stopped producer")
          (assoc this :conn nil))
      this)))

(defn make-producer [config]
  (map->Producer {:config config}))
