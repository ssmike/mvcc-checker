(ns jepsen.yt_models
  (:gen-class)
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [checker :as checker]
                    [util :as util]
                    [generator :as gen]
                    [store :as store]]
            [knossos [model :as model]
                     [op :as op]
                     [linear :as linear]
                     [history :as history]]
            [knossos.linear.report :as report])
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
                            unlocked (assoc op :blocks (:process op))]
                        (if (= (:type write-op) :ok)
                          locked
                          [locked unlocked]))
                      op))))
          (reverse history))
    (->> new-history
        persistent!
        reverse)))

(defn terminate-markers
  [history]
  (let [new-history (transient [])
        seen-processes (transient #{})]
    (run! (fn [op]
            (conj! new-history
              (let [proc (:process op)]
                (if (seen-processes proc)
                  op
                  (do
                    (if (= (:type op) :invoke)
                      (conj! seen-processes proc))
                    (assoc op :terminates proc))))))
      (reverse history))
    (-> new-history
        persistent!
        reverse)))

(def snapshot-serializable
  (reify checker/Checker
    (check [this test model history opts]
      (let [new-history (-> history
                            terminate-markers
                            foldup-locks)
            result (linear/analysis model history)]
        ;; Just aphyr's code from jepsen.checker
        (when-not (:valid? result)
          (util/meh
            ; Renderer can't handle really broad concurrencies yet
            (report/render-analysis!
              history result (.getCanonicalPath (store/path! test (:subdirectory opts)
                                                      "linear.svg")))))
        ; Writing these can take *hours* so we truncate
        (assoc result
               :final-paths (take 10 (:final-paths result))
               :configs     (take 10 (:configs result)))))))
