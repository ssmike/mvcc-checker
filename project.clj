(defproject dyntables "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6-SNAPSHOT" :exclusions [knossos]]
                 [org.clojure/data.json "0.2.6"]
                 [knossos "0.2.9-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]] ;;override aphyr's unpatched version
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
  :profiles {:uberjar {:aot :all}})
