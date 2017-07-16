(ns jepsen.dyntables.checker-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [yt-models :as yt]
                    [checker    :as checker]
                    [generator  :as gen]
                    [tests :as tests]]
            [jepsen.dyntables [checker :as mvcc-checker]]
            [knossos [model :as model]]
            [unilog.config :as unilog]
            [clojure.edn :as edn]
            [jepsen.dyntables.history-gen :refer :all]))

(defn test-valid
  [orig-history]
  (let [result (checker/check
                 mvcc-checker/snapshot-serializable
                 {}
                 yt/empty-locked-dict
                 orig-history
                 {})
        diag-state (:state result)
        diag-hist (:best result)
        message  (str "according to checker history is invalid\n"
                      "state - " diag-state "\n"
                      diag-hist
                      "left " (count diag-hist) " entries out of " (count orig-history) "\n")]
     (is (:valid? result) message)))

(defn test-invalid
  [orig-history]
  (let [result (checker/check
                 mvcc-checker/snapshot-serializable
                 {}
                 yt/empty-locked-dict
                 orig-history
                 {})
        diag-state (:state result)
        diag-hist (:best result)
        message  (str "according to checker history is valid\n"
                      "state - " diag-state "\n"
                      diag-hist
                      "left " (count diag-hist) " entries out of " (count orig-history) "\n")]
     (is (not (:valid? result)) message)))

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
  (test-valid (one-line-success)))

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
  (test-valid (one-line-read-failure)))

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
  (test-valid (one-line-write-failure)))

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
  (test-valid (unacknowledged-commit)))

(defn simple-concurrent-write
  []
  (hist-gen
    (read-op :2 2 1)
    (op-ok :2)
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :2 1 3)
    (op-ok :2)
    (write-op :1 1 2)
    (op-ok :1)))

(deftest simple-concurrent-write-test
  (test-invalid (simple-concurrent-write)))

(defn hanged-concurrent-write
  []
  (hist-gen
    (read-op :2 2 1)
    (op-ok :2)
    (read-op :1 0 1)
    (op-ok :1)
    (write-op :2 1 3)
    (op-hangs :2)
    (write-op :1 1 2)
    (op-ok :1)
    (read-op :3 1 3)
    (op-ok :3)))

(deftest hanged-concurrent-write-test
  (test-invalid (simple-concurrent-write)))

(defn unserializable-valid
  []
  (hist-gen
    (read-op :1 2 1)
    (op-ok :1)
    (read-op :2 1 1)
    (op-ok :2)
    (write-op :1 1 2)
    (op-ok :1)
    (write-op :2 2 3)
    (op-ok :2)))

(deftest unserializable-valid-test
  (test-valid (unserializable-valid)))

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
  (test-valid (timed-out-commit)))

(defn checkfile
  [fname]
  (with-open [in (-> fname io/reader java.io.PushbackReader.)]
    (let [history (take-while identity
                              (repeatedly #(edn/read {:eof nil} in)))]
      (test-valid history))))

(deftest ^:fat check-logs
  (doseq [f (.listFiles (java.io.File. "data"))]
    (testing (.getName f)
      (checkfile (.getPath f)))))

(deftest ^:fat check-dev-log
  (if (.exists (java.io.File. "checker-log"))
      (checkfile "checker-log")
      (do
        (error "can't read checker-log")
        (info "pwd is" (System/getProperty "user.dir")))))

(defn without-debug[f]
  (unilog/start-logging! {:console true :level "info"})
  (f))

(defn to-file[f]
  (unilog/start-logging! {:console false :level "debug"
                          :file "debug.log"})
  (f))

(use-fixtures :each to-file)
