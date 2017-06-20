(ns jepsen.yt
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  [:import java.lang.Runtime])

(defn encode
  [op]
  (let [allowed-keys #{:f :value :rpc-id :req-id}]
    (json/write-str
      (conj {} (filter (fn [[x _]]
                         (contains? allowed-keys x))
                       op)))))

(defn decode
  [msg]
  (let [mp (json/read-str msg)]
    (into {} (map (fn [[a b]] [(keyword a)
                               ((case a
                                  ("value" "rpc-id" "req-id" "ret")
                                    identity
                                    keyword)
                                 b)])
                  mp))))


(defn ysend
  [{:keys [in cache rpc-cnt]} msg]
  (let [result (promise)
        id (swap! rpc-cnt inc)]
    (swap! cache (fn [mp] (assoc mp id result)))
    (binding [*out* in]
      (println (encode (assoc msg :rpc-id id)))
      (flush))
    (let [value @result]
      (swap! cache (fn [mp] (dissoc mp id)))
      (dissoc value :rpc-id))))

(defn close
  [{:keys [reader in] :as client}]
  (ysend client {:f :terminate})
  (future-cancel reader))

(def proxy-num (atom 0))

(defn start-client
  []
  (let [cnt (swap! proxy-num inc)
        proc (. (Runtime/getRuntime) exec (into-array ["run-proxy.sh" (str cnt)]))
        cache (atom {})
        out (io/reader (. proc getInputStream))
        worker (fn []
                (binding [*in* out]
                  (loop []
                    (let [{id :rpc-id :as msg} (decode (read-line))]
                      (deliver (@cache id) msg))
                    (recur))))]
    {:in (io/writer (. proc getOutputStream))
     :cache cache
     :rpc-cnt (atom 0)
     :reader (future (worker))}))

(def table-mounted
  [(ref false)
   (ref nil)])

(defn verify-table-mounted
  [sock]
  (let [[mounted mount-result] table-mounted
        my-promise (promise)]
    (let [[im-first to-deliver]
          (dosync
              (if (not @mounted)
                (do (ref-set mounted true)
                    (ref-set mount-result my-promise)
                 [true my-promise])
                [false @mount-result]))]
      (if im-first (do (deliver to-deliver (ysend sock {:f :mount-table}))
                       @to-deliver)
                   @to-deliver))))

(defn wait-master
  [sock]
  (ysend sock {:f :wait-for-yt}))
