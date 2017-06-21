(ns jepsen.dyntables.util)

(def infinity 1e9)

(defn transitions-count
  [history]
  (->> history 
       (map :value)
       (apply concat)
       (apply max)
       (+ 1)))

(defn models-count
  [edges]
  (->> edges
       (mapcat (fn [it] [(it 0) (it 2)]))
       (apply max)
       (+ 1)))
