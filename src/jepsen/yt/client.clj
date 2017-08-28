(ns jepsen.yt.client
  (:require [jepsen.client :as client]
            [jepsen.util :refer [timeout]]
            [clojure.tools.logging :refer [info warn error debug]]
            [clojure.java.shell :refer [sh]])
  (:import io.netty.channel.nio.NioEventLoopGroup
           java.util.LinkedList
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

(def schema
  (delay
    (-> (TableSchema$Builder.)
        (.addKey "key" ColumnValueType/INT64)
        (.addValue "value" ColumnValueType/INT64)
        .build)))

(defn client
  [opts]
   (reify client/Client

     (setup! [this test node]
       (let [{:keys [host port path] :as rpc-opts} (:rpc-opts test)]
         (info "waiting for mounted table")
         (compare-and-set! mount-table nil (delay (sh "/control/setup-test.sh" path)))
         @mount-table
         (let [rpc-client (-> rpc-opts
                              (rpc-options)
                              (create-client host port))]
           (client (assoc rpc-opts
                          :tx (atom nil)
                          :last-op (atom nil)
                          :rpc-client rpc-client)))))

     (invoke! [this test op]
       (let [{:keys [last-op tx rpc-client path]} opts]
         (try
           (reset! last-op (:f op))
           (merge op
                  (case (:f op)
                    :start-tx
                    (do
                      (as-> rpc-client f
                           (.startTransaction f (ApiServiceTransactionOptions. ETransactionType/TABLET))
                           (.join f)
                           (reset! tx f))
                      (let [req (-> (reduce
                                        (fn [req [key val]]
                                          (.addFilter req (LinkedList. [key]))
                                        (LookupRowsRequest. path @schema)
                                        (:value op)))
                                    (.addLookupColumns (LinkedList. ["key"])))
                            result (-> (.lookupRows @tx req)
                                       .join
                                       .getYTreeRows)]
                        {:value (reduce {}
                                        (fn [map row]
                                          (let [key (-> row (.get "key") .longValue)
                                                val (-> row (.get "value") .longValue)]
                                            (assoc map key val))))}))

                    :commit
                    (if @tx
                      (do
                        (let [req (-> (reduce
                                        (fn [req [key val]]
                                          (.addInsert (list key val)))
                                        (ModifyRowsRequest. path @schema)
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
       (reset! mount-table nil))))
