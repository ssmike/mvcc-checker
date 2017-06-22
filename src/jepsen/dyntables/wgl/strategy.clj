(ns jepsen.dyntables.wgl.strategy
  (:require [clojure.tools.logging :refer [info debug]]))

(defprotocol IExecutionStrategy
  (schedule [_ fun])
  (execute! [_]))

(def ^:dynamic *current-strategy*)

(defmacro cooperate
  [call]
  `(schedule *current-strategy*
             (delay (~@call))))

(defmacro with-strategy
  [strategy & body]
  `(let [strategy# ~strategy]
     (binding [*current-strategy* strategy#]
       (or
         (cooperate ~@body)
         (execute! strategy#)))))

(deftype Recursion[]
  IExecutionStrategy
  (schedule [_ fun] @fun)
  (execute! [_]))

(defn recursion[] (Recursion.))

(deftype DepthFirst[^:unsynchronized-mutable current-call]
  IExecutionStrategy
  (schedule [_ fun]
    (set! current-call (conj current-call fun))
    nil)
  (execute! [_]
    (loop [stack current-call]
      (let [[item & stack] stack
            to-stop @item
            to-insert current-call]
        (debug (str "pushed " (count current-call) " calls"))
        (set! current-call ())
        (or to-stop
            (recur (into stack to-insert)))))))

(defn depth-first[] (DepthFirst. ()))
