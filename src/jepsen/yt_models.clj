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
(defn gen-cell-val [] (rand-int max-cell-val))
(defn gen-key [] (rand-int 3))

(defrecord DynGenerator [writing-processes request-counter]
  gen/Generator
  (op [this test process]
    (merge {:type :invoke}
           (let [id (swap! request-counter inc)]
             (if (contains? @writing-processes process)
               (do
                 (swap! writing-processes disj process)
                 {:req-id id :f :write-and-unlock :value [(gen-key) (gen-cell-val)]})
               (do
                 (swap! writing-processes conj process)
                 {:req-id id :f :read-and-lock :value [(gen-key) nil]}))))))

(defn dyntables-gen [] (DynGenerator. (atom #{})
                                      (atom 0)))

(defrecord LockedDict [dict locks]
  java.lang.Object
  (toString [_]
    (str "internal dict " dict " locks " locks))
  Model
  (step [m op]
    (let [[key val lock] (:value op)]
      (case (:f op)
        :read-and-lock
          (cond
            (locks lock)
              (inconsistent (str "can't lock " lock))
            (not= (dict key) val)
              (inconsistent (str "can't read " val " from " key))
            true
              (let [new-locks (if (nil? lock)
                                locks
                                (conj locks lock))
                    new-dict (assoc dict key val)]
                (LockedDict. new-dict new-locks)))
        :write-and-unlock
          (if (locks key)
            (LockedDict. (assoc dict key val)
                         (disj locks key))
            (inconsistent (str "writing to unlocked " lock)))))))

(def empty-locked-dict (LockedDict. {0 1
                                     1 1
                                     2 1}
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
