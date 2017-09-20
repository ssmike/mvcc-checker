(ns jepsen.mvcc.history-test
  (:require [jepsen.mvcc.history :as history]
            [clojure.test :refer :all]
            [jepsen.mvcc.history-gen :refer :all]
            [clojure.tools.logging :refer [error debug info]]
            [unilog.config :as unilog]))

(defn- rand-register[] (rand-int 5))
(defn- rand-val[] (rand-int 6))

(defmacro rand-branch
  [& branches]
  (let [case-body (apply concat
                    (map-indexed vector branches))]
    `(let [rnd# (rand-int ~(count branches))]
       (case rnd#
         ~@case-body))))

(defn- rand-kvs
  []
  (-> {}
      (conj [(rand-register) (rand-val)])
      (conj [(rand-register) (rand-val)])
      seq
      flatten))

(defn rand-history
  ([processes length]
   (hist-gen
     (rand-history (set (range processes)) length #{} #{})))
  ([processes len unclosed-reads unclosed-ops]
   (if (or (seq unclosed-ops) (> len 0))
     (let [p (if (= len 0)
               (rand-nth (vec unclosed-ops))
               (rand-nth (vec processes)))]
       (if (unclosed-ops p)
         (let [ops (disj unclosed-ops p)]
           (rand-branch
             (do (op-ok p)
                 (recur processes len unclosed-reads ops))
             (do (op-fail p)
                 (recur processes len (disj unclosed-reads p) ops))
             (do (op-hangs p)
                 (recur (-> processes
                            (conj (+ 1 (apply max processes)))
                            (disj p))
                        len (disj unclosed-reads p) ops))))

         (let [ops (conj unclosed-ops p)]
           (if (unclosed-reads p)
             (do (apply read-op (cons p (rand-kvs)))
                 (recur processes
                        (- len 1)
                        (conj unclosed-reads p)
                        ops))
             (do (apply write-op (cons p (rand-kvs)))
                 (recur processes
                        (- len 1)
                        (disj unclosed-reads p)
                        ops)))))))))

(defn check-threads
  [history checker process]
  (testing "checking threads invariants"
    (let [processes (set (map process history))]
      (doseq [p processes]
        (checker (filter
                   (fn[op] (= (process op) p))
                   history))))))

(defn check-index-one-thread
  [history]
  (doseq [[invoke return] (partition-all 2 history)]
    (testing "op types"
      (is (= (:type invoke) :invoke))
      (is (contains? #{:ok :fail :info} (:type return))))
      (testing "indices equality"
        (is (= (:index invoke) (:index return))))))

(defn check-index-uniqueness
  [history]
  (testing "index uniqueness"
    (->> history
        (group-by :index)
        (map (fn [_ v]
               (is (<= (count v) 2)))))))

(deftest index-small
  (let [history (history/index (rand-history 3 8))]
    (testing (str "index history " history)
      (check-threads history check-index-one-thread :process)
      (check-index-uniqueness history))))

(deftest index-large
  (let [history (history/index (rand-history 5 40))]
    (testing (str "index history " history)
      (check-threads history check-index-one-thread :process)
      (check-index-uniqueness history))))

(deftest ^:fat index-fat
  (let [history (history/index (rand-history 30 40000))]
    (testing (str "index history ")
      (check-threads history check-index-one-thread :process)
      (check-index-uniqueness history))))

(defn check-completion-invariants
  [history]
  (testing "completion thread invariants"
    (doseq [[invoke return] (partition-all 2 history)]
      (testing "op types"
        (is (apply = (map :type invoke)))
        (is (= (:type (first invoke)) :invoke))
        (if return
          (is (apply = (map :type return)))
          (is (contains? #{:ok nil} (:type (first return))))))
      (if return
        (testing "ops equality"
          (let [op->transition (fn [op] (select-keys op #{:value :f}))]
            (is (= (map op->transition invoke)
                   (map op->transition return)))))))))

(defn add-alternatives
  [alts history]
  (for [ops history
        :let [op (first ops)]]
    (into ops (for [_ (range alts)]
                (assoc op
                       :value
                       (apply assoc {} (rand-kvs)))))))

(deftest completion-small
  (let [history (->> (rand-history 3 8)
                     (map vector)
                     (add-alternatives 3)
                     history/complete-history)]
    (testing (str "complete foldup history " history)
      (check-threads history check-completion-invariants
                     (comp :process first)))))

(deftest completion-large
  (let [history (->> (rand-history 5 40)
                     (map vector)
                     (add-alternatives 3)
                     history/complete-history)]
    (testing (str "complete foldup history " history)
      (check-threads history check-completion-invariants
                     (comp :process first)))))

(deftest ^:fat completion-fat
  (let [history (->> (rand-history 16 40000)
                     (map vector)
                     (add-alternatives 3)
                     history/complete-history)]
    (testing (str "complete foldup history " history)
      (check-threads history check-completion-invariants
                     (comp :process first)))))
