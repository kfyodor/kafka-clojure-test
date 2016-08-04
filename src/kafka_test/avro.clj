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
           [org.apache.avro.util Utf8]
           [org.apache.avro Schema Schema$Type]))

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

(defn- throw-invalid-type
  [schema obj]
  (throw (Exception. (format "Value `%s` cannot be cast to `%s` schema"
                             (str obj)
                             (.toString schema)))))

(defn- bytes? [x]
  (if (nil? x)
    false
    (-> x class .getComponentType (= Byte/TYPE))))

(defn- clj->java
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
      obj
      (throw-invalid-type schema obj))

    Schema$Type/ARRAY ;; TODO Exception for complex type
    (let [f (partial clj->java (.getElementType schema))]
      (GenericData$Array. schema (map f obj)))

    Schema$Type/FIXED
    (if (and (bytes? obj) (= (count obj) (.getFixedSize schema)))
      (GenericData$Fixed. schema obj)
      (throw-invalid-type schema obj))

    Schema$Type/ENUM
    (let [enums (into #{} (.getEnumSymbols schema))]
      (if (contains? enums obj)
        (GenericData$EnumSymbol. schema obj)
        (throw-invalid-type schema obj)))

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

(defn make-message
  [entity-type value]
  (let [schema (get schemas entity-type)
        topic  (name entity-type)
        record (make-generic-record schema value)]
    {:topic topic
     :key (str (:id value))
     :value (clj->java schema value)}))

(defn maybe-parse-string
  ;; todo: rename to parse-value
  ;; and add more types???
  [v]
  (if (instance? Utf8 v)
    (.toString v)
    v))

(defn parse-message
  [msg]
  (let [fields (map #(.name %)
                    (.. msg (getSchema) (getFields)))]
    (into {}
          (map
           (fn [k]
             [(keyword (->kebab-case k)) (maybe-parse-string
                                          (.get msg k))])
           fields))))

(defn parse-avro-stream
  [stream]
  (map #(update-in % [:value] parse-message) stream))


;; PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
;; NAMED_TYPES =     Set.new(%w[fixed enum record error])
