(ns jepsen.dyntables
  (:gen-class)
  (:import knossos.model.Model)
  (:require [clojure.tools.logging :refer [debug info]]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [clojure.set        :as set]
            [clojure.data.json  :as json]
            [jepsen.os.debian   :as debian]
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
                    [yt         :as yt]
                    [yt-models  :as models]]
            [org.httpkit.client :as http]))

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

(defn client
  [con]
  (reify client/Client
    (setup! [this test node]
        (info "waiting for yt")
        (let [sock (yt/start-client)]
          (info "waiting for master")
          (yt/wait-master sock)
          (info "mounting dyn-table")
          (yt/verify-table-mounted sock)
          (info "yt proxy set up")
          (client sock)))
    (invoke! [this test op]
      (timeout 6500 (assoc op :type :info, :error :timeout)
        (merge op (yt/ysend con op))))
    (teardown! [_ test] (yt/close con))))

(defn two-way-drop
  [test src dst]
  (do
    (future (net/drop! (:net test) test src dst))
    (future (net/drop! (:net test) test dst src))))

(defn silence!
  [test src nodes]
  (doseq [dst nodes]
    (future (net/drop! (:net test) test src dst))))

(defn partition-master-nodes
  [master nodes to-take]
  (let [monitor-port 20000
        statuses (atom (into {} (for [node nodes]
                                  [node false])))
        poller (delay (future ; we aren't going to start polling right now
                (doseq [node (cycle (cons nil nodes))]
                   (if (nil? node)
                     (Thread/sleep 1300)
                     (do
                       ; guilty until proven innocent
                       (swap! statuses assoc node false)
                       (http/get (str "http://" (name node) ":" monitor-port "/orchid/tablet_cells")
                                 {:timeout 1000}
                                 (fn [{:keys [status body error]}]
                                   (cond
                                     error (debug (str "exception thrown while connecting to " node))
                                     (not= status 200) (debug (str node " responded with " status))
                                     (-> body json/read-str empty? not) (swap! statuses assoc node true)
                                     ; do nothing
                                     :else ()))))))))]

    (reify client/Client
      (setup! [this test node]
        @poller
        this)
      (invoke! [this test op]
        (case (:f op)
          :start
          (let [to-split (->> nodes
                              shuffle
                              (filter @statuses)
                              (take to-take)
                              (into #{}))]
            (doseq [node to-split]
              (silence! test node (cons master nodes)))
            (assoc op :value (str "Cut off " to-split)))
          :stop
          (do (net/heal! (:net test) test)
              (assoc op :value "fully connected"))))
      (teardown! [_ test]
        (future-cancel @poller)))))

(defn d-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [pre-test (merge tests/noop-test opts)
        timeout (:time-limit pre-test)
        test (merge pre-test
               {:nodes   [:n1 :n2 :n3 :n4 :n5 :master]
                :name    "Dyntables"
                :os      debian/os
                :db      db
                :client  (client nil)
                :nemesis (partition-master-nodes :master [:n1 :n2 :n3 :n4 :n5] 1)
                :timeout timeout
                :generator (->> (models/dyntables-gen)
                                (gen/stagger 0.2)
                                (gen/nemesis
                                  (gen/seq (cycle [(gen/sleep 4)
                                                   {:type :info, :f :start}
                                                   (gen/sleep 4)
                                                   {:type :info, :f :stop}])))
                                (gen/time-limit timeout))
                :model   models/empty-locked-dict
                :checker (checker/compose
                           {:perf   (checker/perf)
                            :mvcc   models/snapshot-serializable})
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
