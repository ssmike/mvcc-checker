(ns jepsen.dyntables.wgl.strategy-test
  (:require [clojure.test :refer :all]
            [jepsen.dyntables.wgl.strategy :as strategy]))

(defn gen-tree
  [n]
  (map rand-int (range n)))

(defn children
  [tree x]
  (->> tree
       (map vector (range))
       (filter (fn [[_ v]] (= v x)))
       (map first)
       (filter #(not= 0 %))))

(defn test-strategy
  [strategy n]
  (let [g (gen-tree n)
        cache (atom #{})
        explore-node (fn explore-node [node]
                       (swap! cache conj node)
                       (doseq [node (children g node)]
                         (strategy/cooperate
                           (explore-node node)))
                       nil)]
    (strategy/with-strategy strategy
      (explore-node 0))
    (is (= @cache
          (set (range n))))))

(deftest recursion-test
  (test-strategy (strategy/recursion) 10))

(deftest dfs-test
  (test-strategy (strategy/depth-first) 10))

(deftest concurrent-test
  (test-strategy (strategy/concurrent 3) 10))

(deftest ^:fat concurrent-fat-test
  (test-strategy (strategy/concurrent 5) 20000))
