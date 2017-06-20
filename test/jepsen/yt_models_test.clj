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

(def ^:dynamic operation-cache)

(defmacro hist-gen
  [& body]
  `(binding [operation-cache (atom (transient{}))]
     (list ~@body)))

(defn read-op[proc key val]
  (let [op {:type :invoke :f :read-and-lock :value [key val] :process proc}]
    (swap! operation-cache assoc! proc op)
    (assoc op :value [key nil])))

(defn write-op[proc key val]
  (let [op {:type :invoke :f :write-and-unlock :value [key val] :process proc}]
    (swap! operation-cache assoc! proc op)
    op))

(defn op-fail[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (assoc op :type :fail)))

(defn op-hangs[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (assoc op :type :info)))

(defn op-ok[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (assoc op :type :ok)))

(defmacro test-line
  [history]
  `(let [history# (->> ~history
                       foldup-locks
                       complete-history)
         m# (memo/memo yt/empty-locked-dict history#)
         result# (wgl/check
                   (:init m#)
                   (:history m#)
                   (:edges m#))]
     (is (:valid? result#) "according to checker history is invalid")))

(defn one-line-success
  []
  (hist-gen
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :1 2 2)
    (op-ok :1)
    (read-op :1 1 1)
    (op-ok :1)
    (write-op :1 0 0)
    (op-ok :1)))

(deftest one-line-success-correctness
  (test-line (one-line-success)))

(defn one-line-read-failure
  []
  (hist-gen
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :1 2 2)
    (op-ok :1)
    (read-op :1 1 1)
    (op-fail :1)
    (write-op :1 0 0)
    (op-fail :1)))

(deftest one-line-read-failure-correctness
  (test-line (one-line-read-failure)))

(defn one-line-write-failure
  []
  (hist-gen
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :1 2 2)
    (op-ok :1)
    (read-op :1 1 1)
    (op-ok :1)
    (write-op :1 0 0)
    (op-hangs :1)))

(deftest one-line-write-failure-correctness
  (test-line (one-line-write-failure)))

(defn pretty-print
  [list]
  (run! println list))

(defn unacknowledged-commit
  []
  (hist-gen
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :1 1 2)
    (read-op :2 1 2)
    (op-hangs :1)
    (op-ok :2)
    (write-op :2 0 2)
    (op-fail :2)))

(deftest unacknowledged-commit-test
  (test-line (unacknowledged-commit)))

(defn timed-out-commit
  []
  (hist-gen
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :1 1 2)
    (read-op :2 1 1)
    (op-hangs :1)
    (op-ok :2)
    (write-op :2 0 2)
    (op-fail :2)))

(deftest timed-out-commit-test
  (test-line (timed-out-commit)))

(defn checkfile
  [fname]
  (let [history  (->> fname
                      slurp
                      read-string)]
    (test-line history)))

(deftest check-logs
  (if (.exists (java.io.File. "checker-test"))
    (checkfile "checker-test")
    (do
      (error "can't read checker-test")
      (info "pwd is" (System/getProperty "user.dir")))))

(defn without-debug[f]
  (unilog/start-logging! {:console true :level "info"})
  (f))

(use-fixtures :once without-debug)
