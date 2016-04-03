(ns kafka-test.consumer
  (:require [taoensso.timbre :as log]
            [com.stuartsierra.component :as component]))

(defrecord Consumer [config conn]
  component/Lifecycle

  (start [this]
    (log/info "TODO: implement consumer")
    this)
  (stop  [this]
    this))

(defn make-consumer [config]
  (map->Consumer {:config config}))
