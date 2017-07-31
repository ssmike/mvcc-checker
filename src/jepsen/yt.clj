(ns jepsen.yt
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [jepsen.store :as store])
  (:import java.lang.Runtime))

(def encode str)
(def decode edn/read-string)

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
  [test]
  (let [cnt (swap! proxy-num inc)
        proc (.exec (Runtime/getRuntime)
                (into-array ["run-proxy.sh" (.getCanonicalPath
                                              (store/path! test (str "proxy-" cnt ".log")))]))
        cache (atom {})
        out (io/reader (.getInputStream proc))
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
              (if-not @mounted
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
