(defproject dyntables "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6-SNAPSHOT"]
                 [org.clojure/data.json "0.2.6"]
                 [spootnik/unilog "0.7.13"]
                 [org.clojure/core.match "0.3.0-alpha4"]]
  :main jepsen.dyntables
  :target-path "target/%s"
  :jvm-opts ["-Xmx50g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-XX:+UnlockCommercialFeatures"
;             "-XX:-OmitStackTraceInFastThrow"
             "-server"]
  :omit-source true
  :profiles {:uberjar {:aot :all}})
