(ns jepsen.dyntables.wgl
  (:require [jepsen.dyntables.util :as util]
            [clojure.tools.logging :refer [error debug debug]])
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
            (conj! res {:index n
                        :value (:value op)
                        :max-index infinity})
            (assoc! cache (:process op) n))
          (let [saved (cache (:process op))
                old-item (res saved)
                new-item (assoc old-item :max-index n)]
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
       (let [_ (debug "looking up in cache")
             item (MemoizationItem. state linearized)
             seen (and (empty? skipped)
                       (cache item))
             _ (conj! cache item)
             _ (debug "not found in cache")]
         seen)
       {:valid? false}

       ;; try to linearize op
       :else
       (let [lin (merge-results
                    (for [t (:value op)
                          :let [v (aget G state t)]
                          :when (not= v -1)]
                      (let [_ (debug "linearizing" op)
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
    (explore index (make-sequential history) init)))
