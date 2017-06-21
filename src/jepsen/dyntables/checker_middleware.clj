(ns jepsen.dyntables.checker-middleware
  (:require [clojure.java.shell :as sh]
            [jepsen.dyntables.util :as util]
            [jepsen.dyntables.history :refer [make-sequential *id-index-mapping*]]
            [clojure.string :refer [join]]))

(defn str-history
  [history]
  (cons
    (count history)
    (flatten
      (for [h history]
        [(join " "  [({:invoke 0 :ok 1} (:type h))
                     (:index h)
                     (:max-index h)])
         (map first (:value h))
         (map second (:value h))]))))

(defn str-edges
  [edges]
  (cons
    (count edges)
    (map (partial join " ") edges)))

(defn format-lines
  [lines]
  (join "\n" lines))

(defn make-stdin
  [init history edges]
  (format-lines (concat [init]
                        (str-edges edges)
                        (str-history history))))

(defn dump-logs!
  [init history edges]
  (let [mapping (atom (transient {}))
        seq-hist (binding [*id-index-mapping* mapping]
                   (make-sequential history))
        in (make-stdin init seq-hist edges)
        _ (spit "external-checker-test.log" in)]
    {:valid? true
     :state 0
     :best ()}))

(defn run-checker!
  [init history edges]
  (let [mapping (atom (transient {}))
        seq-hist (binding [*id-index-mapping* mapping]
                   (make-sequential history))
        in (make-stdin init seq-hist edges)
        res ((comp read-string :out)
              (sh/sh "checker" :in in))]
    (merge {:valid? (= 0 (:exit res))
            :state res
            :best (map @mapping (:best res))})))
