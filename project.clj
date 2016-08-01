(defproject kafka-test "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.stuartsierra/component "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.13"]
                 [environ "1.0.2"]
                 [com.taoensso/timbre "4.3.1"]
                 [faker "0.2.2"]
                 [org.apache.kafka/kafka-clients "0.10.0.0"]
                 [prismatic/schema "1.1.2"]]
  :main ^:skip-aot kafka-test.core
  :target-path "target/%s")
