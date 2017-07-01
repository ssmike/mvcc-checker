(ns jepsen.dyntables.wgl.util
  (:require [clojure.tools.logging :refer [debug info warn]]))

(def infinity 1e9)

(defn transitions-count
  [history]
  (if (empty? history)
    (do (warn "There were 0 successful transitions. Seems suspicious.") 0)
    (->> history
         (map :value)
         (apply concat)
         (apply max)
         (+ 1))))

(defn models-count
  [edges]
  (->> edges
       (mapcat (fn [it] [(it 0) (it 2)]))
       (cons 0)
       (apply max)
       (+ 1)))
