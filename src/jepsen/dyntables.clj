(ns jepsen.dyntables
  (:gen-class)
  (:import knossos.model.Model)
  (:require [clojure.tools.logging :refer [debug info]]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [clojure.set        :as set]
            [jepsen.os          :as os]
            [jepsen [db         :as db]
                    [cli        :as cli]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [generator  :as gen]
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]
                    [net        :as net]
                    [yt-models  :as models]]
            [jepsen.yt.client   :as yt-client]
            [jepsen.dyntables.checker :as mvcc-checker]
            [jepsen.yt.nemesis :refer [partition-master-nodes]]))

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (c/exec :bash "/jepsen/run.sh")
        (info (str node " set up"))))
    (teardown! [_ test node]
      (c/su
        (c/exec :bash "/jepsen/stop.sh")
        (info (str node " teardown!"))))
    db/LogFiles
      (log-files [_ test node]
        (case node
          :master ["/master/master.debug.log" "/master/master.log"]
          ["/node/node.debug.log" "/node/node.log"]))))

(defn d-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [pre-test (merge tests/noop-test opts)
        timeout (:time-limit pre-test)
        test (merge pre-test
               {:nodes   [:n1 :n2 :n3 :n4 :n5 :master]
                :name    "Dyntables"
                :os      os/noop
                :db      db
                :rpc-opts {:host "::" :port 3000 :path "//table"}
                :client  (yt-client/client nil);(client nil)
                :nemesis (partition-master-nodes :master [:n1 :n2 :n3 :n4 :n5] 1)
                :timeout timeout
                :generator (->> (models/dyntables-gen)
                                (gen/stagger 0.2)
                                (gen/nemesis
                                  (gen/seq (cycle [(gen/sleep 1)
                                                   {:type :info, :f :start}
                                                   (gen/sleep 2)
                                                   {:type :info, :f :stop}])))
                                (gen/time-limit timeout))
                :model   models/empty-locked-dict
                :checker (checker/compose
                           {:perf   (checker/perf)
                            :mvcc   mvcc-checker/snapshot-serializable})
                :ssh {:username "root",
                      :strict-host-key-checking false,
                      :private-key-path "~/.ssh/yt"}})]
    test))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn d-test})
                   (cli/serve-cmd))
            args))
