(ns jepsen.mvcc.history
  (:require [clojure.tools.logging :refer [info debug]]))

(defn index
  "Assign unique index to each operation and its return record."
  [history]
  (-> (reduce (fn [[index invoke-cache result] op]
                (if (= (:type op) :invoke)
                  [(inc index)
                   (assoc! invoke-cache (:process op) index)
                   (conj! result (assoc op :req-id index))]
                  [index
                    invoke-cache
                    (conj! result (assoc op
                                         (:req-id op)
                                         (invoke-cache (:process op))))]))
              [0 (transient {}) (transient [])]
              history)
      (get 2)
      persistent!))

(defn- write-set
  [write-op]
  (-> write-op :value keys set))

(defn foldup-locks
  "Successful commit implies that start-tx locks write-set.
  If commit fails start-tx shouldn't lock anything.
  And in case of :info we have to preserve both histories.
  ATTENTION: We don't override record types and processes."
  [history]
  (->> (reduce (fn [[history last-write :as skip] op]
                 (let [write-op (last-write (:process op))]
                   (case (:f op)
                     :start-tx
                     [(conj! history
                             (if (and write-op (not= (:type write-op) :fail))
                                (let [locked (assoc op :locks (write-set write-op))
                                      unlocked (assoc op :blocks true)]
                                  (if (= (:type write-op) :ok)
                                    [locked]
                                    [unlocked locked]))
                                 [op]))
                      last-write]

                     :commit
                     [(conj! history [(assoc op :locks (write-set op))])
                      (if (not= (:type op) :invoke)
                        (assoc! last-write (:process op) op)
                        last-write)]

                     skip)))

                     [(transient []) (transient {})] (reverse history))
       first
       persistent!
       reverse))

(defn- merge-success
  [invoke-ops ok-ops]
  (if (nil? ok-ops)
    invoke-ops
    (mapv (fn [invoke-op ok-op]
            (assoc invoke-op :value (:value ok-op)))
          invoke-ops
          ok-ops)))

(defn complete-history
  "Here we delete faled ops. Also bring :value from each
  return record to invoke one."
  [history]
  (let [reduction
        (reduce (fn [[cache history] op]
                   (let [p (:process (first op))
                         items
                         (case (-> op first :type)
                           :ok [[p op] op]
                           :fail [[p :fail] nil]
                           :info [[p nil] nil]
                           :invoke [[p nil]
                                    (let [saved (cache p)]
                                     (if (not= saved :fail)
                                       (merge-success op saved)))])]
                     (mapv conj! [cache history] items)))
                [(transient {}) (transient [])]
                (reverse history))
        [cache history] (map persistent! reduction)]
    (assert (->> cache vals (filter identity) empty?)
            "not all operations reduced")
    (->> history (filter identity) reverse)))
