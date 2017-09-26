(defproject com.ssmike/mvcc-checker "1.0-SNAPSHOT"
  :description "mvcc checker compatible with jepsen framework"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6"]
                 [org.clojure/data.json "0.2.6"]
                 [spootnik/unilog "0.7.21"]]
  :target-path "target/%s"
  :jvm-opts ["-Xmx10g"
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
  :plugins [[jonase/eastwood "0.2.4"]
            [lein-kibit "0.1.5"]]
  :omit-source true
  :test-selectors {:default (complement :fat)
                   :all (constantly true)}
  :profiles {:uberjar {:aot :all}})
