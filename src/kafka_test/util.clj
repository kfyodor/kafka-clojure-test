(ns kafka-test.util
  (:require [schema.core :as s]
            [clojure.string :as str])
  (:import [java.util Properties]))

(s/defschema PropsMap
  {s/Keyword s/Any})

(s/defn make-props :- Properties
  [props-map :- PropsMap]
  (let [props (Properties.)]
    (doseq [[k v] props-map]
      (.setProperty props
                    (name k)
                    (if (coll? v)
                      (str/join "," v)
                      (str v))))
    props))
