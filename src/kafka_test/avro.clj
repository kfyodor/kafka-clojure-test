(ns kafka-test.avro
  (:require [clojure.java.io :as io]
            [cheshire.core :as json]
            [camel-snake-kebab.core :refer [->kebab-case ->snake_case]])
  (:import [org.apache.avro.generic GenericData GenericData$Record]
           [org.apache.avro Schema]))

;; this is test code: load all schemas into a Clojure map
(def schemas
  (->> (file-seq (io/file (io/resource "avro/")))
       (filter #(re-matches #"\A.+\.avsc\z" (.getName %)))
       (map #(let [schema (slurp %)
                   {:keys [name]} (json/parse-string schema true)]
               [(-> name ->kebab-case keyword)
                (Schema/parse schema)]))
       (into {})
       (doall)))

(defn- make-generic-record [^Schema schema record]
  (reduce (fn [avro-record [k v]]
            (let [key (-> k name ->snake_case)]
              (.put avro-record key v)
              avro-record))
          (GenericData$Record. schema)
          record))

(defn make-message
  [entity-type value]
  (let [schema (get schemas entity-type)
        topic  (name entity-type)
        record (make-generic-record schema value)]
    {:topic topic
     :key (str (:id value))
     :value record}))
