(ns jepsen.dyntables.wgl
  (:require [jepsen.dyntables [util :refer :all]
                              [history :refer [make-sequential *id-index-mapping*]]]
            [jepsen.dyntables.wgl [context :as ctx]
                                  [strategy :as strategy]]
            [clojure.tools.logging :refer [debug info]])
  (:import [java.util BitSet]))

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
       false
       (let [op# (first col#)]
         (or op# (recur (next col#)))))))

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

(defn explore
  ([G history state]
  (let [n (->> history (map :index) distinct count)
        ctx (ctx/transient-ctx)
        exp-res (strategy/with-strategy (strategy/depth-first)
                  (explore ctx
                           G
                           (empty-linearized n)
                           '()
                           (into (list {:index infinity
                                        :max-index infinity})
                                 (reverse history))
                           state
                           infinity
                           0))]
    (ctx/found! ctx [n [state history]])
    (debug (str "best found " (ctx/best ctx)))
    {:valid? exp-res :best ((ctx/best ctx) 1)}))

  ([ctx G linearized skipped history state max-index lin-cnt]
   (let [op (first history)
         tail (rest history) ]
     (debug (str "calling explore: first -- " op
                 " max-index " max-index))
     (cond
       (empty? history)
       true

       (> (:index op) max-index)
       (do
         (debug "gave up because of max-index")
         false)

       ;; try to linearize op
       :else
       (let [lin (merge-results
                    (for [[t to-delete] (:value op)
                          :let [_ (debug (str "state " state " transition " t))
                                v (aget G state t)]
                          :when (not= v -1)]
                      (let [_ (debug "linearizing" op)
                            linearized (to-linearized linearized (:index op) to-delete)
                            item (MemoizationItem. v linearized)
                            _ (debug "looking up in cache")]
                        (if (ctx/seen? ctx item)
                          (debug "already have been here")
                          (let [_ (ctx/found! ctx [lin-cnt [state history]])
                                _ (debug "constructing history")
                                tail (remove-index tail to-delete)
                                history (into tail skipped)]
                            (strategy/cooperate
                              (explore ctx G linearized '() history v infinity (+ 1 lin-cnt))))))))
             _ (debug "results merged")]

          (or lin
              (debug "failed to linearize")
              ; just skip
              (let [_ (debug "skipping" op)
                    skipped (conj skipped op)
                    max-index (min max-index (:max-index op))]
                (recur ctx G linearized skipped tail state max-index lin-cnt))))))))

(defn check
  [init history edges]
  (let [models (models-count edges)
        transitions (transitions-count history)
        ; just don't know how to properly create two-dimensional array of ints
        index (into-array (for [_ (range models)]
                            (int-array transitions -1)))]
    (doseq [[u t v] edges]
      (aset (aget index u) t v))
    (debug "graph size" models "x" transitions)
    (info "starting wgl")
    (let [mapping (atom (transient {}))
          result (binding [*id-index-mapping* mapping]
                   (explore index (make-sequential history) init))
          [state hist] (:best result)
          mapping (persistent! @mapping)]
      (assoc result :best [state (map (comp mapping :index) hist)]))))
