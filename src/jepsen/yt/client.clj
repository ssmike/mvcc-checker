(ns jepsen.yt.client
  (:require [jepsen.client :as client]
            [jepsen.util :refer [timeout]]
            [clojure.tools.logging :refer [info warn error debug]]
            [clojure.java.shell :refer [sh]]
            [jepsen.store :as store])
  (:import io.netty.channel.nio.NioEventLoopGroup
           java.util.LinkedList
           java.util.function.BiFunction
           ru.yandex.yt.ytclient.bus.DefaultBusConnector
           ru.yandex.yt.ytclient.bus.DefaultBusFactory
           ru.yandex.yt.ytclient.proxy.ApiServiceClient
           ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient
           ru.yandex.yt.ytclient.rpc.RpcOptions
           ru.yandex.yt.ytclient.tables.TableSchema$Builder
           ru.yandex.yt.ytclient.tables.ColumnSchema
           ru.yandex.yt.ytclient.proxy.LookupRowsRequest
           ru.yandex.yt.ytclient.proxy.ModifyRowsRequest
           ru.yandex.yt.ytclient.tables.ColumnValueType
           ru.yandex.yt.ytclient.ytree.YTreeBuilder
           ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions
           ru.yandex.yt.rpcproxy.ETransactionType
           java.net.InetSocketAddress
           java.lang.Runtime))

; I think 32 is big enough. To properly choose value
; we have to investigate netty's code... Meh
(def bus-connector (delay (-> (NioEventLoopGroup. 32)
                              (DefaultBusConnector.))))

(defn create-client
  [opts host port]
  (as->
    (reify java.util.function.Supplier
       (get [_] (InetSocketAddress. host port)))
    f
    (DefaultBusFactory. @bus-connector f)
    (DefaultRpcBusClient. f)
    (ApiServiceClient. f opts)))

(defn with-auth
  [user token]
  (fn [client] (.withTokenAuthentication client user token)))

(defn rpc-options
  [opts]
  (let [{:keys [def-timeout def-request-ack]} opts]
    (-> (RpcOptions.)
        (.setDefaultTimeout (java.time.Duration/ofSeconds (or def-timeout 3)))
        (.setDefaultRequestAck (or def-request-ack false)))))

(def mount-table (atom nil))

(def write-schema
  (delay
    (-> (TableSchema$Builder.)
        (.addKey "key" ColumnValueType/INT64)
        (.addValue "value" ColumnValueType/INT64)
        .build)))

(def lookup-schema
  (delay
    (-> (TableSchema$Builder.)
        (.addKey "key" ColumnValueType/INT64)
        .build)))


(defn client
  [opts]
   (reify client/Client

     (setup! [this test node]
       (let [{:keys [host port path] :as rpc-opts} (:rpc-opts test)]
         (info "waiting for mounted table")
         (compare-and-set! mount-table nil (delay (sh "/control/setup-test.sh" path)))
         (deref @mount-table)
         (let [rpc-client (-> rpc-opts
                              (rpc-options)
                              (create-client host port))]
           (client (assoc rpc-opts
                          :tx (atom nil)
                          :last-op (atom nil)
                          :logs-copied (atom nil)
                          :rpc-client rpc-client)))))

     (invoke! [this test op]
       (let [{:keys [last-op tx rpc-client path]} opts]
         (try
           (reset! last-op (:f op))
           (merge op
                  (case (:f op)
                    :start-tx
                    (let [req (-> (reduce
                                      (fn [req [key val]]
                                        (.addFilter req (LinkedList. [key])))
                                      (LookupRowsRequest. path @lookup-schema)
                                      (:value op))
                                  (.addLookupColumns (LinkedList. ["key"])))
                          result (-> rpc-client
                                   (.startTransaction (ApiServiceTransactionOptions. ETransactionType/TABLET))
                                   (.handleAsync
                                     (reify BiFunction
                                       (apply [_ transaction err]
                                         (if err (throw err))
                                         (let [result (-> transaction
                                                          (.lookupRows req)
                                                          .join
                                                          .getYTreeRows)]
                                           (reset! tx transaction)
                                           result))))
                                   .join)]
                      {:value (reduce (fn [map row]
                                        (let [key (-> row (.get "key") .longValue)
                                              val (-> row (.get "value") .longValue)]
                                          (assoc map key val)))
                                      {}
                                      result)})

                    :commit
                    (if @tx
                      (do
                        (let [req (-> (reduce
                                        (fn [req [key val]]
                                          (.addInsert (list key val)))
                                        (ModifyRowsRequest. path @write-schema)
                                        (:value op)))
                              _ (-> (.modifyRows rpc-client req) .join)
                              _ (-> @tx .commit .join)])
                        (reset! tx nil))
                      {:type :fail})))
           (catch Exception e
             (.printStackTrace e)
             (reset! tx nil)
             (if (#{:commit :write} @last-op)
               (assoc op :type :info, :error :timeout)
               (assoc op :type :fail))))))

     (teardown! [_ test]
       (reset! mount-table nil)
       (let [log (.getCanonicalPath (store/path! test (str "proxy")))]
         (compare-and-set! (:logs-copied opts)
                           nil
                           (delay (doseq [ext [".log", ".debug.log"]]
                                    (sh "cp" (str "/control/proxy" ext) (str log ext)))))))))
