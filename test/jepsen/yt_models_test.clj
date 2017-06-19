(ns jepsen.yt-models-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [yt-models :as yt]
                    [checker    :as checker]
                    [generator  :as gen]
                    [tests :as tests]]
            [jepsen.dyntables.memo :as memo]
            [jepsen.dyntables.checker-middleware :as middleware]
            [jepsen.dyntables.wgl :as wgl]
            [knossos [model :as model]]))

(defn set-process
  ([history]
    (set-process history 1))

  ([history proc]
    (map #(assoc % :process proc) history)))

(def one-line-success
  (set-process
    [{:type :invoke :f :read-and-lock :value [0 nil]}
     {:type :ok :f :read-and-lock :value [0 1]}
     {:type :invoke :f :write-and-unlock :value [2 2]}
     {:type :ok :f :write-and-unlock :value [2 2]}
     {:type :invoke :f :read-and-lock :value [1 nil]}
     {:type :ok :f :read-and-lock :value [1 1]}
     {:type :invoke :f :write-and-unlock :value [0 0]}
     {:type :ok :f :write-and-unlock :value [0 0]}]))

(def one-line-read-failure
  (set-process
    [{:type :invoke :f :read-and-lock :value [0 nil]}
     {:type :ok :f :read-and-lock :value [0 1]}
     {:type :invoke :f :write-and-unlock :value [2 2]}
     {:type :ok :f :write-and-unlock :value [2 2]}
     {:type :invoke :f :read-and-lock :value [1 nil]}
     {:type :fail :f :read-and-lock :value [1 nil]}
     {:type :invoke :f :write-and-unlock :value [0 0]}
     {:type :fail :f :write-and-unlock :value [0 0]}]))

(def one-line-write-failure
  (set-process
    [{:type :invoke :f :read-and-lock :value [0 nil]}
     {:type :ok :f :read-and-lock :value [0 1]}
     {:type :invoke :f :write-and-unlock :value [2 2]}
     {:type :ok :f :write-and-unlock :value [2 2]}
     {:type :invoke :f :read-and-lock :value [1 nil]}
     {:type :ok :f :read-and-lock :value [1 1]}
     {:type :invoke :f :write-and-unlock :value [0 0]}
     {:type :info :f :write-and-unlock :value [0 0]}]))

(defn pretty-print
  [list]
  (run! println list))

;; just ensure that history generators don't throw exceptions
(deftest one-line-history-test
  (run! (fn [hist] (-> hist
                       yt/foldup-locks
                       yt/complete-history
                       pretty-print)
                   (println))
       [one-line-success
        one-line-read-failure
        one-line-write-failure]))

(defmacro test-line
  [history]
  `(->> ~history
        yt/foldup-locks
        (linear/analysis yt/empty-locked-dict)))

(deftest memo-test
  (run! (fn [hist] (let [m (->> hist
                                yt/foldup-locks
                                yt/complete-history
                                (memo/memo yt/empty-locked-dict))]
                     (is (:valid?
                           (wgl/check
                             (:init m)
                             (:history m)
                             (:edges m))))))
       [one-line-success
        one-line-read-failure
        one-line-write-failure]))

;(deftest checker-test
;  (is (:valid? (test-line one-line-success)))
;  (is (:valid? (test-line one-line-read-failure)))
;  (is (:valid? (test-line one-line-write-failure))))

