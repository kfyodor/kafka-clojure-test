(ns kafka-test.source
  (:require [clojure.core.async :refer [>! <! go-loop timeout] :as async]
            [faker.name :as name-gen]
            [faker.phone-number :as phone-gen]
            [com.stuartsierra.component :as component]))

(defn- random-message [counter]
  {:id         counter
   :first-name (name-gen/first-name)
   :last-name  (name-gen/last-name)
   :phone      (first (phone-gen/phone-numbers))})

(defn- start-generator!
  [ch]
  (go-loop [cnt 0]
    (>! ch (random-message cnt))
    (recur (inc cnt))))

(defrecord Source [ch gen]
  component/Lifecycle

  (start [this]
    (when (nil? ch)
      (let [ch  (async/chan 10)
            gen (start-generator! ch)]
        (assoc this :ch ch :gen gen))))

  (stop [this]
    (when ch (async/close! ch))
    (when gen (async/close! gen))
    (assoc this :ch nil :gen nil)))

(defn make-source []
  (map->Source {}))
