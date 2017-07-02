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
      (if-not (empty? stack)
        (let [[item & stack] stack
              to-stop @item
              to-insert current-call]
          (debug (str "pushed " (count current-call) " calls"))
          (set! current-call ())
          (or to-stop
              (recur (into stack to-insert))))))))

(defn depth-first[] (DepthFirst. ()))

(deftype Concurrent[threads local-cache in-flight]
  IExecutionStrategy
  (schedule [_ fun]
    (swap! in-flight inc)
    (swap! local-cache conj fun)
    nil)

  (execute! [_]
    (let [search-success (atom false)
          queue (atom '(nil ()))
          broadcaster (fn[]
                        (let [[fetched _] (swap! queue
                                                 (fn [[_ q]]
                                                   (list (first q)
                                                         (rest q))))]
                          (or fetched
                              (do
                                (debug "worker idle")
                                (Thread/sleep 200)
                                ()))))

          granularity (* threads 10)

          worker-fn (fn [cache]
                      (when (and (not @search-success)
                                 (> @in-flight 0))
                         (doseq [item (broadcaster)]
                           (debug "running item")
                           (if @item (reset! search-success true))
                           (swap! in-flight dec))
                         (doseq [batch (partition-all granularity (reverse @cache))]
                           (swap! queue (fn [[_ q]]
                                          (list nil (conj q batch)))))
                         (reset! cache ())
                         (recur cache)))

          workers (for [_ (range threads)]
                    (future
                      (let [cache (atom ())]
                        (binding [*current-strategy* (Concurrent. nil cache in-flight)]
                          (worker-fn cache)))))]
      (reset! queue (list nil (list @local-cache)))
      (reset! local-cache nil) ; to ensure that nobody touches parent context
      (debug "running workers")
      (doall workers)
      ; join them
      (run! deref workers)
      @search-success)))

(defn concurrent[threads] (Concurrent. threads (atom ()) (atom 0)))

(defn factory
  [opts]
  (case (:execution-strategy opts)
    :concurrent (concurrent (:threads opts))
    :recursive (recursion)
    :dfs (depth-first)))
