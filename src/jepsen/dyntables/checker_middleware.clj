(ns jepsen.dyntables.checker-middleware
  (:require [clojure.java.shell :as sh]
            [jepsen.dyntables.util :as util]))

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

(defn make-stdin
  [init history edges]
  (format-lines (concat [(str init)
                         (str (util/models-count edges) " "
                              (util/transitions-count edges))]
                        (str-edges edges)
                        (str-history history))))

(defn dump-logs!
  [init history edges]
  (do
    (spit "jepsen-checker-log"
          (make-stdin init history edges))
    {:valid? true}))

(defn run-checker!
  [init history edges]
  (let [in (make-stdin init history edges)
        res (sh/sh "checker" :in in)]
    {:valid? (= 0 (:exit res))
     :diags (:out res)}))
