(ns jepsen.dyntables.wgl.history
  (:require [jepsen.dyntables.wgl.util :refer :all]))

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
