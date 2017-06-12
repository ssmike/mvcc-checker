(ns jepsen.yt_models-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [yt_models :as models]
                    [checker    :as checker]
                    [generator  :as gen]
                    [tests :as tests]]
            [knossos.linear :as linear]))

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

(defn mix-histories
  [histories])

(defn pretty-print
  [list]
  (run! println list))

(deftest one-line-success-test
  (run! (fn [hist] (-> hist
                       models/terminate-markers
                       models/foldup-locks
                       pretty-print)
                   (println))
       [one-line-success
        one-line-read-failure
        one-line-write-failure]))

(def jepsen-mock-test
  (merge tests/noop-test {:name "mock-test"}))

(defmacro test-line
  [history]
  `(->> ~history
        models/terminate-markers
        models/foldup-locks
        (linear/analysis models/empty-locked-dict)))

(deftest checker-test
  (is (:valid? (test-line one-line-success)))
  (is (:valid? (test-line one-line-read-failure)))
  (is (:valid? (test-line one-line-write-failure))))

