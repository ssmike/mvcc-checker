(ns jepsen.dyntables.checker-middleware
  (:require [clojure.java.shell :as sh]))

(defn str-history
  [history]
  (for [h history]
    (str " " ({:invoke 0 :ok 1} (:type h))
         " " (:process h) 
         " " (:value h))))

(def str-edges (partial map str))

(defn format-lines
  [lines]
  (let [builder (java.lang.StringBuilder. "")]
    (doseq [line lines]
      (. builder append (str line "\n")))
    (str builder)))

(defn dump-logs!
  [history edges]
  (do
    (spit "jepsen-checker-log"
          (format-lines (concat (str-edges edges)
                                (str-history history))))
    {:valid? true}))

(defn run-checker!
  [history transitions]
  (let [in (format-lines (concat (str-edges transitions)
                                 (str-history history)))
        res (sh/sh "checker" :in in)]
    {:valid? (= 0 (:exit res))
     :diags (:out res)}))
