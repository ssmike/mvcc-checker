(ns jepsen.dyntables.wgl
  (:require [jepsen.dyntables.util :refer :all]
            [jepsen.dyntables.history :refer [make-sequential *id-index-mapping*]]
            [clojure.tools.logging :refer [debug info]])
  (:import [java.util BitSet]
           [java.util LinkedList]))

(defrecord MemoizationItem [^int model ^BitSet linearized])

(defn empty-linearized[n] (BitSet. n))

(defn to-linearized
  [^BitSet set op to-block]
  (let [new-set ^BitSet (.clone set)]
    (.set new-set op)
    (if (pos? to-block)
      (.set new-set to-block))
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
       (assert (seq col#))
       (let [item# (first col#)]
         (if (= (:index item#) ~index)
           (into (rest col#) acc#)
           (recur (conj acc# item#)
                  (rest col#)))))))

(def ^:dynamic *lin-cache*)

(def ^:dynamic *best-found*)

(defn update-found
  [[cnt1 set1 :as item1] [cnt2 set2 :as item2]]
  (if (> cnt1 cnt2)
    item1
    item2))

(defn explore
  ([G history state]
   (let [n (->> history (map :index) distinct count)]
     (debug "history to explore" history)
     (binding [*lin-cache* (transient #{})
               *best-found* (atom [0 nil])]
       (let [exp-res (explore (empty-linearized n)
                              G
                              '()
                              (into (list {:index infinity
                                           :max-index infinity})
                                    (reverse history))
                              state
                              infinity
                              0)]
         (assoc exp-res :best (@*best-found* 1))))))
  ([linearized G skipped history state max-index lin-cnt]
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
                  seen (*lin-cache* item)
                  _ (debug "not found in cache")]
              (conj! *lin-cache* item)
              (swap! *best-found* update-found [lin-cnt [state history]])
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
                            linearized (to-linearized linearized (:index op) to-delete)]
                        (explore linearized G '() history v max-index (+ 1 lin-cnt)))))
             _ (debug "results merged")]

          (if (:valid? lin)
            lin
            ; just skip
            (let [_ (debug "skipping")
                  skipped (conj skipped op)
                  max-index (min max-index (:max-index op))]
              (recur linearized G skipped tail state max-index lin-cnt))))))))

(defn check
  [init history edges]
  (let [models (models-count edges)
        transitions (transitions-count edges)
        ; just don't know how to properly create two-dimensional array of ints
        index (into-array (for [_ (range models)]
                            (int-array transitions -1)))]
    (doseq [[u t v] edges]
      (aset (aget index u) t v))
    (info "starting wgl")
    ;(doseq [i history]
    ;  (info "history item" i))
    (let [mapping (atom (transient {}))
          result (binding [*id-index-mapping* mapping]
                   (explore index (make-sequential history) init))
          [state hist] (:best result)
          mapping (persistent! @mapping)
          _ (info mapping)]
      (assoc result :best [state (map (comp mapping :index) hist)]))))
