(ns kafka-test.core
  (:require [kafka-test
             [simple-producer :as simple]
             [consumer :as c]
             [source   :as s]]
            [environ.core :refer [env]]
            [com.stuartsierra.component :as component])
  (:gen-class))

(defn- add-shutdown-hook!
  [f]
  (doto (Runtime/getRuntime)
    (.addShutdownHook (Thread. f))))

(def config
  {:kafka-bootstrap-server (or (env :kafka-bootstrap-server)
                               "0.0.0.0:9092")

   :schema-registry-url (or (env :schema-registry-url)
                            "http://localhost:8081")})

(defn make-system [config]
  (component/system-map
   :source   (s/make-source)
   :producer (component/using
              (simple/make-simple-producer config)
              [:source])
   :consumer (c/make-consumer config)))

(defn -main
  [& args]
  (.setContextClassLoader (Thread/currentThread) nil)

  (let [system (component/start (make-system config))
        lock   (promise)]
    (add-shutdown-hook! #(do (component/stop system)
                             (deliver lock :done)))
    @lock
    (System/exit 0)))
