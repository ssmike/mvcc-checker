(ns jepsen.mvcc.checker
  (:require [clojure.tools.logging :refer [debug info]]
            [clojure.java.io :as io]
            [jepsen.checker :as checker]
            [jepsen.mvcc [history :as history]
                              [wgl     :as wgl]
                              [history :as history]
                              [memo    :as memo]]))

(defn -diagnostics
  ([orig-history diag-history num]
   (->> (-diagnostics orig-history diag-history)
        (take num)
        (into '())))
  ([orig-history diag-history]
   (let [ids (set diag-history)]
      (->> orig-history
           (filter (comp ids :req-id))
           (map #(dissoc % :req-id))))))

(def snapshot-serializable
  (reify checker/Checker
    (check [this test model orig-history opts]
      (let [indexed-history (-> orig-history history/index)
            history (-> indexed-history
                        history/foldup-locks
                        history/complete-history)
            memo (memo/memo model history)
            res (wgl/check
                  (:init memo)
                  (:history memo)
                  (:edges memo))
            [diag-state diag-hist] (:best res)]
        (assoc res :state ((:models memo) diag-state)
                   :best (-diagnostics
                           indexed-history
                           diag-hist
                           20))))
    java.lang.Object
    (toString [_]
      "[snapshot isolation checker]")))
