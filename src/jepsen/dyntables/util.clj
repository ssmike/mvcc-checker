(ns jepsen.dyntables.util) 

(defn transitions-count
  [edges]
  (->> edges
      (map second)
      distinct
      count))

(defn models-count
  [edges]
  (->> edges
       (mapcat (fn [it] [(it 0) (it 2)]))
       distinct
       count))

