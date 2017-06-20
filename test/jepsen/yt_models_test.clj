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
            [jepsen.dyntables.history :refer :all]
            [knossos [model :as model]]
            [unilog.config :as unilog]))

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
                       foldup-locks
                       complete-history
                       pretty-print)
                   (println))
       [one-line-success
        one-line-read-failure
        one-line-write-failure]))

(defmacro test-line
  [history]
  `(->> ~history
        foldup-locks
        (linear/analysis yt/empty-locked-dict)))

(deftest memo-test
  (run! (fn [hist] (let [m (->> hist
                                foldup-locks
                                complete-history
                                (memo/memo yt/empty-locked-dict))]
                     (is (:valid?
                           (wgl/check
                             (:init m)
                             (:history m)
                             (:edges m))))))
       [one-line-success
        one-line-read-failure
        one-line-write-failure]))

(defn checkfile
  [fname]
  (let [history  (->> fname
                      slurp
                      read-string
                      foldup-locks
                      complete-history)
        m (memo/memo yt/empty-locked-dict history)]
    (wgl/check (:init m) (:history m) (:edges m))))

(deftest check-logs
  (if (.exists (java.io.File. "checker-test"))
    (is (:valid?
          (checkfile "checker-test")))
    (error "file doesn't exist")))

(defn without-debug[f]
  (unilog/start-logging! {:console true :level "info"})
  (f))

;(use-fixtures :once without-debug)
