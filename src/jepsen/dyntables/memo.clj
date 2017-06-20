(ns jepsen.dyntables.memo
  "Here we build transitions graph for use in heavy C++ checker"
  (:require [knossos.model :as model]
            [clojure.tools.logging :refer [info debug]])
  (:import (knossos.model Model)))

(defn op->transition
  [op]
  (select-keys op #{:f :value}))

(defn all-transitions
  [history]
  (->> history
       flatten
       (filter #(= :invoke (:type %)))
       (map op->transition)))

(defn expand-models
  [models-set transitions]
  (into models-set
        (for [model models-set
              transition transitions
              :let [target (model/step model transition)]
              :when (not (model/inconsistent? target))]
          target)))


(defn all-models
  [init history transitions]
  (loop [models #{init}]
    (let [new-models (expand-models models transitions)]
      (if (not= (count models)
                (count new-models))
        (recur new-models)
        models))))

(defn preprocess-history
  [history transition-index]
  (mapv (fn [ops]
          (let [op (first ops)
                transitions (map op->transition ops)
                blocks (map (comp boolean :blocks) ops)]
            {:value (mapv transition-index transitions)
             :blocks (vec blocks)
             :type (:type op)
             :process (:process op)
             :req-id (:req-id op)}))
        history))

(defn memo
  [init history]
  (let [_ (info "memoizing model")
        transitions (vec (all-transitions history))
        models (vec (all-models init history transitions))
        swap (fn [i op] [op i])
        model-index (into {} (map-indexed swap models))
        transition-index (into {} (map-indexed swap transitions))]
    {:init (model-index init)
     :transitions transitions
     :models models
     :history (preprocess-history history transition-index)
     :edges (vec (for [source models
                       j (range (count transitions))
                       :let [transition (transitions j)
                             target (model/step source transition)]
                       :when (not (model/inconsistent? target))]
                   [(model-index source)
                    j
                    (model-index target)]))}))
