(ns jepsen.dyntables.wgl
  (:require [jepsen.dyntables.util :as util]
            [clojure.tools.logging :refer [error info debug]])
  (:import [java.util BitSet]
           [java.util LinkedList]))

(def infinity 1e9)

(defn make-sequential
  [history]
  (let [cache (transient {})
        res (transient [])]
    (doseq [op history]
      (let [n (count res)]
        (if (= :invoke (:type op))
          (do
            ; fill locks
            (when-let [saved (cache (:process op))]
              (let [old-item (res saved)
                    _ (debug "old item" old-item)
                    _ (debug "blocks" (:blocks op))
                    new-value (mapv (fn [v block]
                                      (if block (assoc v 1 n) v))
                                    (:value old-item)
                                    (:blocks old-item))
                    _ (debug "new item" new-value)]
                (assoc! res saved (assoc old-item :value new-value))))
            ; conj :invoke op
            (debug "op value" (:value op))
            (debug "conj value" (mapv (fn[x] [x -1]) (:value op)))
            (conj! res {:index n
                        :value (mapv (fn[x] [x -1]) (:value op))
                        :max-index infinity})
            (assoc! cache (:process op) n))
          (let [saved (cache (:process op))
                old-item (res saved)
                new-item (assoc old-item :max-index (- n 1))]
            (assoc! res saved new-item)))))
    (->> res
         persistent!
         reverse
         (into '()))))

(defrecord MemoizationItem [^int model ^BitSet linearized])

(defn empty-linearized[n] (BitSet. n))

(defn to-linearized
  [^BitSet set op]
  (let [new-set ^BitSet (.clone set)]
    (.set new-set op)
    new-set))

(defmacro merge-results
  [results]
  `(loop [col# (seq ~results)]
     (if (empty? col#)
       {:valid? false}
       (let [op# (first col#)]
         (if (:valid? op#)
           op#
           (recur (next col#)))))))

(defmacro remove-index
  [history index]
  `(if (= ~index -1)
     ~history
     (loop [acc# (list)
            col# (seq ~history)]
       (let [item# (first col#)]
         (if (= (:index item#) ~index)
           (into (rest col#) acc#)
           (recur (conj acc# item#)
                  (rest col#)))))))

(defn explore
  ([G history state]
   (let [n (->> history (map :index) distinct count)]
     (explore (transient #{})
              (empty-linearized n)
              G
              '()
              (into (list {:index infinity
                           :max-index infinity})
                    history)
              state
              infinity)))
  ([cache linearized G skipped history state max-index]
   (let [op (first history)
         tail (rest history) ]
     (debug "calling explore" state)
     (debug "first -- " op)
     (cond
       (empty? history)
       {:valid? true}

       (> (:index op) max-index)
       {:valid? false}

       ; we have already been here
       ; this check makes sense only if we are at first history item
       (and (empty? skipped)
            (let [_ (debug "looking up in cache")
                  item (MemoizationItem. state linearized)
                  seen (cache item)
                  _ (debug "not found in cache")]
              (conj! cache item)
              seen))
       {:valid? false}

       ;; try to linearize op
       :else
       (let [lin (merge-results
                    (for [[t to-delete] (:value op)
                          :let [v (aget G state t)]
                          :when (not= v -1)]
                      (let [_ (debug "linearizing" op)
                            _ (debug "removing" to-delete "from" tail)
                            tail (remove-index tail to-delete)
                            history (into tail skipped)
                            linearized (to-linearized linearized (:index op))]
                        (explore cache linearized G '() history v max-index))))
             _ (debug "results merged")]
          (if (:valid? lin)
            lin
            ; just skip
            (let [_ (debug "skipping")
                  skipped (conj skipped op)
                  max-index (max max-index (:max-index op))]
              (recur cache linearized G skipped tail state max-index))))))))

(defn check
  [init history edges]
  (let [models (util/models-count edges)
        transitions (util/transitions-count edges)
        ; just don't know how to properly create two-dimensional array of ints
        index (into-array (for [_ (range models)]
                            (int-array transitions -1)))]
    (doseq [[u t v] edges]
      (aset (aget index u) t v))
    (debug index)
    (doseq [item history]
      (debug item))
    (doseq [item (make-sequential history)]
      (debug item))
    (explore index (make-sequential history) init)))
