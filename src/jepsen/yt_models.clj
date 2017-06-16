(ns jepsen.yt-models
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [util :as util]
                    [generator :as gen]
                    [store :as store]
                    [checker :as checker]]
            [knossos.model :as model]
            [jepsen.dyntables.memo :as memo]
            [jepsen.dyntables.checker-middleware :as middleware])
  (:import (knossos.model Model)))

(def inconsistent model/inconsistent)

(def max-cell-val 3)
(defn gen-cell-val [] (rand-int max-cell-val))
(defn gen-key [] (rand-int 3))

(defrecord DynGenerator [writing-processes]
  gen/Generator
  (op [this test process]
    (merge {:type :invoke}
      (if (contains? @writing-processes process)
        (do
          (swap! writing-processes disj process)
          {:f :write-and-unlock :value [(gen-key) (gen-cell-val)]})
        (do
          (swap! writing-processes conj process)
          {:f :read-and-lock :value [(gen-key) nil]})))))

(defn dyntables-gen [] (DynGenerator. (atom #{})))

(defrecord LockedDict [dict locks]
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

(defn foldup-locks
  [history]
  (let [last-write (transient {})
        new-history (transient [])]
    (run! (fn [op]
            (if (and (not= (:type op) :invoke)
                     (= (:f op) :write-and-unlock))
              (assoc! last-write (:process op) op))
            (conj! new-history
                (let [op-val (:value op)
                      write-op (last-write (:process op))]
                    (if (and (= (:f op) :read-and-lock)
                             (not (nil? write-op))
                             (not= (:type write-op) :fail))
                      (let [locked (assoc op :value (conj op-val
                                                     ;; we are locking written cell
                                                     ((:value write-op) 0)))
                            unlocked (assoc op :blocks true)]
                        (if (= (:type write-op) :ok)
                          [locked]
                          [unlocked locked]))
                      [op]))))
          (reverse history))
    (-> new-history
        persistent!
        reverse)))

(defn merge-success
  [invoke-ops ok-ops]
  (mapv (fn [invoke-op ok-op]
          (assoc invoke-op :value (:value ok-op))) invoke-ops ok-ops))

(defn complete-history
  [history]
  (let [cache (transient {})
        new-history (transient [])]
    (doseq [op (reverse history)]
      (let [p (:process op)]
        (case (-> op first :type)
          :ok (do
                (assoc! cache p op)
                (conj! new-history op))
          :fail (assoc! cache p :fail)
          :info (assoc! cache p op)
          :invoke (if (not= (cache p) :fail)
                    (conj! new-history
                           (merge-success op
                                          (cache p)))))))
    (-> new-history
        persistent!
        reverse)))

(def snapshot-serializable
  (reify checker/Checker
    (check [this test model orig-history opts]
      (let [history (-> orig-history
                        foldup-locks
                        complete-history)
            memo (memo/memo model history)
            ;res (middleware/run-checker! (:history memo)
            ;                          (:transitions memo))
            res (middleware/dump-logs! (:history memo)
                                       (:transitions memo))]
        (with-open [w (io/writer "jepsen-op-log")]
          (doseq [h orig-history]
            (.write w (str h "\n"))))
        res))))
