(ns jepsen.dyntables.checker-middleware
  (:require [clojure.java.shell :as sh]))

(defn str-history
  [history]
  (concat
    [(count history)]
    (for [h history]
      (str " " ({:invoke 0 :ok 1} (:type h))
           " " (:process h)
           " " (:value h)))))

(defn str-edges
  [edges]
  (concat
    [(count edges)]
    (map str edges)))

(defn format-lines
  [lines]
  (let [builder (java.lang.StringBuilder. "")]
    (doseq [line lines]
      (. builder append (str line "\n")))
    (str builder)))

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

(defn make-stdin
  [history edges]
  (format-lines (concat [(str (models-count edges) " "
                              (transitions-count edges))]
                        (str-edges edges)
                        (str-history history))))

(defn dump-logs!
  [history edges]
  (do
    (spit "jepsen-checker-log"
          (make-stdin history edges))
    {:valid? true}))

(defn run-checker!
  [history edges]
  (let [in (make-stdin history edges)
        res (sh/sh "checker" :in in)]
    {:valid? (= 0 (:exit res))
     :diags (:out res)}))
