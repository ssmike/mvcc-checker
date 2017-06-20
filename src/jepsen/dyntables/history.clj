(ns jepsen.dyntables.history
  (:require [clojure.tools.logging :refer [info debug]]
            [clojure.core.match :refer [match]]
            [jepsen.dyntables.util :refer :all]))

(defn foldup-locks
  [history]
  (let [last-write (atom (transient {}))]
    (->> (reverse history)
         (map (fn [op]
                (do
                  (let [op-val (:value op)
                        write-op (@last-write (:process op))]
                    (match [(:f op) (:type op)]
                           [:read-and-lock _]
                           (if (and write-op
                                    (not= (:type write-op) :fail))
                             (let [_ (assert (:value write-op) (str write-op op))
                                   locked (assoc op :value (conj op-val
                                                                 ;; we are locking written cell
                                                                 ((:value write-op) 0)))
                                   unlocked (assoc op :blocks true)]
                               (if (= (:type write-op) :ok)
                                 [locked]
                                 [unlocked locked]))
                             [op])
                           [:write-and-unlock :invoke]
                           [op]
                           [:write-and-unlock _]
                           (do
                             (swap! last-write assoc! (:process op) op)
                             [op])
                           :else nil)))))
         (filter vector?)
         reverse)))

(defn merge-success
  [invoke-ops ok-ops]
  (if (nil? ok-ops)
    invoke-ops
    (mapv (fn [invoke-op ok-op]
            (assoc invoke-op :value (:value ok-op)))
          invoke-ops
          ok-ops)))

(defn complete-history
  [history]
  (let [cache (atom (transient {}))
        history  (->> (reverse history)
                      (map (fn [op]
                             (let [p (:process (first op))]
                               (case (-> op first :type)
                                 :ok (do
                                       (swap! cache assoc! p op)
                                       op)
                                 :fail (swap! cache assoc! p :fail)
                                 :info (swap! cache dissoc! p)
                                 :invoke (let [saved (@cache p)]
                                           (swap! cache dissoc! p)
                                           (if (not= saved :fail)
                                             (merge-success op saved)))))))
                      (filter vector?)
                      reverse)]
    (assert (empty? (persistent! @cache)))
    history))

(def ^:dynamic *id-index-mapping* (atom nil))

(defn make-sequential
  [history]
    (reset! *id-index-mapping* (transient {}))
    (->>
      (let [cache (atom (transient {}))]
        (reduce
          (fn [res op]
            (let [n (count res)]
              (if (= :invoke (:type op))
                (do
                  ; fill locks
                  (when-let [saved (@cache (:process op))]
                    (let [old-item (res saved)
                          new-value (mapv (fn [v block]
                                            (if block (assoc v 1 n) v))
                                          (:value old-item)
                                          (:blocks old-item))]
                      (assoc! res saved (assoc old-item :value new-value))))
                  ; conj :invoke op
                  (swap! cache assoc! (:process op) n)
                  (swap! *id-index-mapping* assoc! n (:req-id op))
                  (conj! res {:index n
                              :value (mapv (fn[x] [x -1]) (:value op))
                              :max-index infinity
                              :blocks (:blocks op)}))
                (let [saved (@cache (:process op))
                      _ (assert (= (:type op) :ok) op)
                      old-item (res saved)
                      new-item (assoc old-item :max-index (- n 1))]
                  (assoc! res saved new-item)))))
        (transient [])
        history))

      persistent!
      (map #(dissoc % :blocks))
      reverse
      (into '())))
