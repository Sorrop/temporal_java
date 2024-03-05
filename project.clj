(defproject temporal_java "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [io.temporal/temporal-sdk "1.22.3"]
                 [metosin/jsonista "0.3.8"]]
  :main ^:skip-aot temporal-java.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
