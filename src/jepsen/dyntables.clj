(ns jepsen.dyntables
  (:gen-class)
  (:import knossos.model.Model)
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [jepsen [db         :as db]
                    [cli        :as cli]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [generator  :as gen]
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]
            [jepsen.yt          :as yt]
            [knossos.model :as knossos]))

(def inconsistent knossos/inconsistent)

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
      (timeout 3000 (assoc op :type :info, :error :timeout)
        (merge op (yt/ysend con op))))
    (teardown! [_ test] (yt/close con))))

(defrecord Dict [dict]
  Model
  (step [m op]
    (let [op-val (:value op)
          {key "key" val "val"} op-val]
      (case (:f op)
        :dyn-table-read (do
                          (assert key (str op))
                          (if (= (get dict key) val)
                          m
                          (inconsistent (str "can't read " val " with key " key))))
        :dyn-table-write (Dict. (assoc dict key val))
        :dyn-table-cas  (let [[from-key to-key] key
                              [from-val to-val] val]
                              (assert to-val (str op val))
                              (assert to-key (str op key))
                              (assert from-key (str op key))
                              (if (contains? dict from-key)
                                (let [res (mod (+ to-val (get dict from-key)) 5)]
                                  (Dict. (assoc dict to-key res)))
                                (inconsistent (str "can't cas with " from-key))))))))

(defn empty-dict
  []
  (Dict. {}))

(def yt-keys
  {0 2
   1 21
   2 31})

(defn rand-key
  []
  (get yt-keys (rand-int 3)))

(defn r-gen   [_ _] {:type :invoke :f :dyn-table-read
                     :value {"key" (rand-key)}})
(defn w-gen   [_ _] {:type :invoke :f :dyn-table-write
                     :value {"val" (rand-int 5) :key (rand-key)}})
(defn cas-gen [_ _] {:type :invoke :f :dyn-table-cas
                     :value {"val" [(rand-int 5) (rand-int 5)]
                             "key" [(rand-key) (rand-key)]}})


(defn d-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "Creating test" opts)
  (merge tests/noop-test
         opts
         {:nodes   [:n1 :n2 :n3 :n4 :n5 :master]
          :name    "Dyntables"
          :os      debian/os
          :db      db
          :client  (client nil)
          :nemesis (nemesis/partition-random-halves)
          :timeout 60
          :generator (->> (gen/mix [r-gen w-gen cas-gen cas-gen])
                          (gen/stagger 1)
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 9)
                                             {:type :info, :f :start}
                                             (gen/sleep 9)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit 60))
          :model   (empty-dict)
          :checker (checker/compose
                     {:perf   (checker/perf)
                      :linear checker/linearizable})
          :ssh {:username "root",
                :strict-host-key-checking false,
                :private-key-path "~/.ssh/yt"}}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn d-test})
                   (cli/serve-cmd))
            args))
