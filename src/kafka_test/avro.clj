(ns kafka-test.avro
  (:require [clojure.java.io :as io]
            [cheshire.core :as json]
            [camel-snake-kebab.core :refer [->kebab-case ->snake_case]])
  (:import [org.apache.avro.generic
            GenericData
            GenericData$Record
            GenericData$Array
            GenericData$Fixed
            GenericData$EnumSymbol]
           [java.nio ByteBuffer]
           [org.apache.avro.util Utf8]
           [org.apache.avro Schema Schema$Type]))

;; this is test code: load all schemas into a Clojure map

(def sample-user
  {:id 1
   :first_name "Test"
   :last_name "Test"
   :phone "+79031234567"
   :order_ids [1,2,3,4]
   :cleaner_id nil
   :address {:id 2
             :city "Moscow"
             :street {:name "Lenina"
                      :house "1"
                      :apt   "2"}}
   :guid (->> (java.util.UUID/randomUUID)
              (str)
              (map byte)
              (byte-array))
   :bytes (byte-array (map byte "test-bytes"))
   :role "USER"})

(def schemas ;; loads all schemas, should be moved to a library
   (->> (file-seq (io/file (io/resource "avro/")))
          (filter #(re-matches #"\A.+\.avsc\z" (.getName %)))
          (map #(let [schema (slurp %)
                      {:keys [name]} (json/parse-string schema true)]
                  [(-> name ->kebab-case keyword)
                   (Schema/parse schema)]))
          (into {})
          (doall)))

(defn- throw-invalid-type
  [schema obj]
  (throw (Exception. (format "Value `%s` cannot be cast to `%s` schema"
                             (str obj)
                             (.toString schema)))))

(defn- bytes? [x]
  (if (nil? x)
    false
    (-> x class .getComponentType (= Byte/TYPE))))

(defn- clj->java ;; todo: test default values???
  "Converts Clojure data structures
   to Avro-compatible Java class"
  [schema obj]
  (condp = (and (instance? Schema schema) (.getType schema))
    Schema$Type/NULL
    (if (nil? obj)
      nil
      (throw-invalid-type schema obj))

    Schema$Type/INT
    (if (and (integer? obj) (<= Integer/MIN_VALUE obj Integer/MAX_VALUE))
      (int obj)
      (throw-invalid-type schema obj))

    Schema$Type/LONG
    (if (and (integer? obj) (<= Long/MIN_VALUE obj Long/MAX_VALUE))
      (long obj)
      (throw-invalid-type schema obj))

    Schema$Type/FLOAT
    (if (float? obj)
      (float obj)
      (throw-invalid-type schema obj))

    Schema$Type/DOUBLE
    (if (float? obj)
      (double obj)
      (throw-invalid-type schema obj))

    Schema$Type/BOOLEAN
    (if (instance? Boolean obj) ;; boolean? added only in 1.9 :(
      obj
      (throw-invalid-type schema obj))

    Schema$Type/STRING
    (if (string? obj)
      obj
      (throw-invalid-type schema obj))

    Schema$Type/BYTES
    (if (bytes? obj)
      (doto (ByteBuffer/allocate (count obj))
        (.put obj)
        (.position 0))
      (throw-invalid-type schema obj))

    Schema$Type/ARRAY ;; TODO Exception for complex type
    (let [f (partial clj->java (.getElementType schema))]
      (GenericData$Array. schema (map f obj)))

    Schema$Type/FIXED
    (if (and (bytes? obj) (= (count obj) (.getFixedSize schema)))
      (GenericData$Fixed. schema obj)
      (throw-invalid-type schema obj))

    Schema$Type/ENUM
    (let [enum (name obj)
          enums (into #{} (.getEnumSymbols schema))]
      (if (contains? enums enum)
        (GenericData$EnumSymbol. schema enum)
        (throw-invalid-type schema enum)))

    Schema$Type/MAP ;; TODO Exception for complex type
    (zipmap (map (comp name ->snake_case) (keys obj))
            (map (partial clj->java (.getValueType schema))
                 (vals obj)))

    Schema$Type/UNION
    (let [[val matched]
          (reduce (fn [_ schema]
                    (try
                      (reduced [(clj->java schema obj) true])
                      (catch Exception _
                        [nil false])))
                  [nil false]
                  (.getTypes schema))]
      (if matched
        val
        (throw-invalid-type schema obj)))

    Schema$Type/RECORD
    (reduce-kv
     (fn [record k v]
       (let [k (-> (name k) ->snake_case)
             s (some-> (.getField schema k)
                       (as-> f (.schema f)))]
         (doto record
           (.put k (clj->java (or s k) v)))))
     (GenericData$Record. schema)
     obj)

    (throw (Exception. (format "Field `%s` is not in schema" schema)))))

(defn- java->clj
  "Parses deserialized Avro object into
   Clojure data structures. Keys in records
   and maps + enums will get keywordized and
   kebab-cased."
  [msg]
  (condp instance? msg
    Utf8
    (str msg)

    java.nio.ByteBuffer
    (.array msg)

    GenericData$Array
    (into [] (map java->clj msg))

    GenericData$Fixed
    (.bytes msg)

    GenericData$EnumSymbol
    (keyword (str msg))

    java.util.HashMap
    (zipmap (map (comp keyword ->kebab-case str) (keys msg))
            (map java->clj (vals msg)))

    GenericData$Record
    ;; Transients are making this slower, I wonder why?
    (loop [fields (seq (.. msg getSchema getFields)) record {}]
      (if-let [f (first fields)]
        (let [n (.name f)
              v (java->clj (.get msg n))
              k (-> n ->kebab-case keyword)]
          (recur (rest fields)
                 (assoc record k v)))
        record))

    msg))

(defn make-message
  [entity-type value]
  (let [schema (get schemas entity-type)
        topic  (name entity-type)
        record (clj->java schema value)]
    {:topic topic
     :key (str (:id value))
     :value (clj->java schema value)}))

(def parse-message java->clj)

(defn parse-avro-stream
  [stream]
  (map #(update-in % [:value] parse-message) stream))
