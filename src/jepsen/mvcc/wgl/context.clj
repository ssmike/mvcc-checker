(ns jepsen.mvcc.wgl.context
  (:import [java.util.concurrent ConcurrentHashMap]))

(defn update-found
  [[cnt1 set1 :as item1] [cnt2 set2 :as item2]]
  (if (> cnt1 cnt2)
    item1
    item2))

(defprotocol IExplorationCtx
  (found! [_ item])
  (best [_])
  (seen?[_ item]))

(defrecord TransientCtx[lin-cache best-found]
  IExplorationCtx
  (found! [_ item]
    (swap! best-found update-found item))
  (best [_] @best-found)
  (seen? [_ item] (if-not (@lin-cache item)
                    (do
                      (swap! lin-cache conj! item)
                      false)
                    true)))

(defn transient-ctx[] (TransientCtx. (atom (transient #{}))
                                     (atom [-1 nil])))

(defrecord ConcurrentCtx[^ConcurrentHashMap lin-cache best-found]
  IExplorationCtx
  (found! [_ item]
    (swap! best-found update-found item))
  (best [_] @best-found)
  (seen? [_ item] (.put lin-cache item true)))

(defn concurrent-ctx[threads] (ConcurrentCtx. (ConcurrentHashMap. 1000 0.5 threads)
                                              (atom [-1 nil])))
