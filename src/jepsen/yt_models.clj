(ns jepsen.yt_models
  (:gen-class)
  (:require [clojure.set :as set]
            [knossos.model :as knossos]
            [jepsen.checker :as checker])
  (:import (knossos.model Model)))

(def inconsistent knossos/inconsistent)

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
                         (if (contains? dict from-key)
                           (let [res (mod (+ to-val (get dict from-key)) 5)]
                             (Dict. (assoc dict to-key res)))
                           (inconsistent (str "can't cas with " from-key))))))))

(def empty-dict (Dict. {}))

(defrecord LockedDict [dict locks]
  Model
  (step [m op]
    (let [op-val (:value op)
          {:keys [entries to-unlock to-lock]} op-val
          new-locks (set/difference (into locks to-lock) to-unlock)
          new-dict (into dict entries)
          bad-locks (set/intersection to-lock locks)
          bad-unlocks (set/difference to-unlock locks)]
      (assert (empty? (set/intersection to-lock to-unlock)) "don't know how to lock and unlock at the same time")
      (if (and (empty? bad-locks) (empty? bad-unlocks))
        (if (and (= (:f op) :read) (not= new-dict dict))
            (inconsistent "inconsistent read " entries " with state " dict)
            (LockedDict. new-dict new-locks))
        (inconsistent "can't change lock state: locking " bad-locks " unlocking " bad-unlocks)))))


(def ^:dynamic current-history)
(def ^:dynamic current-operation)

(defmacro with-history
  "Executes body with empty history and returns result"
  [op & body]
  `(binding [current-history (transient [])
             current-operation ~op]
     ~@body
     (persistent! current-history)))

(defmacro conj-op
  [value & args]
  `(do
     (conj! current-history (assoc current-operation :value ~value ~@args :type :invoke))
     (conj! current-history (assoc current-operation :value ~value ~@args))))

(defmacro _lock
  "lock keys"
  [keys & args]
  `(conj-op {:to-lock ~keys}
            :type :ok
            :f :lock
            ~@args))

(defmacro _unlock
  "unlock key"
  [keys & args]
  `(conj-op {:to-unlock ~keys}
            :type :ok
            :f :lock
            ~@args))

(defmacro _read
  [entries & args]
  `(conj-op {:entries ~entries}
            :type :ok
            :f :read
            ~@args))

(defmacro _write
  [entries & args]
  `(conj-op {:entries ~entries}
            :type :ok
            :f :write
            ~@args))

(defmacro _pass
  []
  `(conj! current-history current-operation))

;; Delete *-cas operations splitting them and introduce write-locks
(def dyntables-checker
  (reify checker/Checker
    (check [self test model history opts]
      (assert (= LockedDict (class model)))
      (let [process-item (fn [op]
                           (with-history op
                            (let [from-val (:ret op)
                                  op-val (:value op)
                                  {key "key" val "val"} op-val]
                             (case (:f op)
                               :dyn-table-read (_read {key val} :type (:type op))
                               :dyn-table-write (do
                                                  (_lock #{key})
                                                  (_write {key val} :type (:type op))
                                                  (_unlock #{key}))
                               :dyn-table-cas  (let [[from-key to-key] key
                                                     [_ to-val] val
                                                     new-val #(mod (+ to-val from-val) 5)]
                                                 (case (:type op)
                                                   :ok (do
                                                         (_lock #{to-key})
                                                         (_read {from-key from-val})
                                                         (_write {to-key (new-val)})
                                                         (_unlock #{to-key}))
                                                   :fail (do
                                                           (_read {from-key from-val})
                                                           ;; ensure that somebody has taken the lock
                                                           (_unlock #{to-key})
                                                           (_lock #{to-key}))
                                                   :info (if (not (nil? from-val))
                                                           (do
                                                             (_lock #{to-key})
                                                             (_read {from-key from-val})
                                                             ;; TODO: knossos research
                                                             (_write {to-key (new-val)} :type :info)
                                                             (_unlock #{to-key})))))
                               (_pass)))))
            new-history
              (->> history
                (filter #(not= (:type %) :invoke))
                (map process-item)
                (reduce concat)
                (vec))]
        (checker/check checker/linearizable test model new-history opts)))))
