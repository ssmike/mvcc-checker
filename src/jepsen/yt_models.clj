(ns jepsen.yt-models
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error debug]]
            [jepsen [util :as util]
                    [generator :as gen]
                    [store :as store]
                    [checker :as checker]]
            [jepsen.dyntables.memo :as memo]
            [jepsen.dyntables.checker-middleware :as middleware]
            [jepsen.dyntables.wgl :as wgl]
            [jepsen.dyntables.history :refer [foldup-locks complete-history]]
            [knossos.model :as model])
  (:import (knossos.model Model)))

(def inconsistent model/inconsistent)

(def max-cell-val 3)
(def cells-count 5)

(defn gen-cell-val [] (rand-int max-cell-val))
(defn gen-key
  ([] (rand-int cells-count))
  ([k] (->> (range cells-count) shuffle (take k))))

(defrecord DynGenerator [writing-processes request-counter]
  gen/Generator
  (op [this test process]
    (merge {:type :invoke}
           (let [id (swap! request-counter inc)
                 [k1 k2] (gen-key 2)]
             (if (contains? @writing-processes process)
               (do
                 (swap! writing-processes disj process)
                 {:req-id id :f :write-and-unlock :value [[[k1 (gen-cell-val)]
                                                           [k2 (gen-cell-val)]]]})
               (do
                 (swap! writing-processes conj process)
                 {:req-id id :f :read-and-lock :value [[[k1 nil]
                                                        [k2 nil]]]}))))))

(defn dyntables-gen [] (DynGenerator. (atom #{})
                                      (atom 0)))

(defrecord LockedDict [dict locks]
  java.lang.Object
  (toString [_]
    (str "internal dict " dict " locks " locks))
  Model
  (step [m op]
    (let [[kvs op-locks] (:value op)
          op-locks (into #{} op-locks)]
      (case (:f op)
        :read-and-lock
          (cond
            (not (empty (set/intersection op-locks locks)))
              (inconsistent (str "can't lock " op-locks))
            (not= (into dict kvs) dict)
              (inconsistent (str "can't read " (vec kvs)))
            true
              (let [new-locks (set/union locks op-locks)
                    new-dict (into dict kvs)]
                (LockedDict. new-dict new-locks)))
        :write-and-unlock
          (if (set/subset? op-locks locks)
            (LockedDict. (into dict kvs)
                         (set/difference locks op-locks))
            (inconsistent (str "writing to unlocked " op-locks)))))))

(def empty-locked-dict (LockedDict. (into {} (for [i (range cells-count)]
                                               [i 1]))
                                    #{}))
(defn -diagnostics
  [orig-history diag-history]
  (let [ids (into #{} diag-history)]
    (->> orig-history
         (filter (comp ids :req-id))
         (take 20))))

(def snapshot-serializable
  (reify checker/Checker
    (check [this test model orig-history opts]
      (let [history (-> orig-history
                        foldup-locks
                        complete-history)
            memo (memo/memo model history)
            _ (spit "checker-test.log" (str orig-history)) ; for yt-models-test/check-logs
            _ (with-open [w (io/writer "jepsen-op-log")]
                (doseq [h orig-history]
                   (.write w (str h "\n"))))
            res (wgl/check
                  (:init memo)
                  (:history memo)
                  (:edges memo))
            [diag-state diag-hist] (:best res)]
        (assoc res :state ((:models memo) diag-state)
                   :best (-diagnostics
                           orig-history
                           diag-hist))))))
