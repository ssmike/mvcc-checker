(ns jepsen.dyntables.history-gen)
            

(def ^:dynamic current-history)
(def ^:dynamic operation-cache)
(def ^:dynamic req-id)

(defmacro hist-gen
  [& body]
  `(binding [operation-cache (atom (transient{}))
             current-history (atom (transient []))
             req-id (atom 0)]
     ~@body
     (persistent! @current-history)))

(defn read-op[proc & kvs]
  (let [kvs (->> kvs (partition-all 2) (map vec) (into {}))
        id (swap! req-id inc)
        op {:type :invoke :f :start-tx :value kvs :process proc :req-id id}]
    (swap! operation-cache assoc! proc op)
    (swap! current-history conj!
      (assoc op :value (map (fn[[x _]] [x nil]) kvs)))))

(defn write-op[proc & kvs]
  (let [kvs (->> kvs (partition-all 2) (map vec) (into {}))
        id (swap! req-id inc)
        op {:type :invoke :f :commit :value kvs :process proc :req-id id}]
    (swap! operation-cache assoc! proc op)
    (swap! current-history conj! op)))

(defn op-fail[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (swap! current-history conj!
      (assoc op :type :fail))))

(defn op-hangs[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (swap! current-history conj!
      (assoc op :type :info))))

(defn op-ok[proc]
  (let [op (@operation-cache proc)]
    (swap! operation-cache dissoc! proc)
    (swap! current-history conj!
      (assoc op :type :ok))))
