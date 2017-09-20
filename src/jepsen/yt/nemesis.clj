(ns jepsen.yt.nemesis
  (:require [clojure.tools.logging :refer [debug info]]
            [clojure.data.json  :as json]
            [jepsen [net    :as net]
                    [client :as client]]
            [org.httpkit.client :as http]))

(defn two-way-drop
  [test src dst]
  (do
    (future (net/drop! (:net test) test src dst))
    (future (net/drop! (:net test) test dst src))))

(defn silence!
  [test src nodes]
  (doseq [dst nodes]
    (future (net/drop! (:net test) test src dst))))

(defn partition-master-nodes
  [master nodes to-take]
  (let [monitor-port 20000
        statuses (atom (into {} (for [node nodes]
                                  [node false])))
        poller (delay (future ; we aren't going to start polling right now
                (doseq [node (cycle (cons nil nodes))]
                   (if (nil? node)
                     (Thread/sleep 1300)
                     (do
                       ; guilty until proven innocent
                       (swap! statuses assoc node false)
                       (http/get (str "http://" (name node) ":"
                                      monitor-port "/orchid/tablet_cells")
                                 {:timeout 1000}
                                 (fn [{:keys [status body error]}]
                                   (cond
                                     error
                                     (debug (str "exception thrown while connecting to " node))

                                     (not= status 200)
                                     (debug (str node " responded with " status))

                                     (-> body json/read-str empty? not)
                                     (swap! statuses assoc node true)

                                     ; do nothing
                                     :else ()))))))))]

    (reify client/Client
      (setup! [this test node]
        @poller
        this)
      (invoke! [this test op]
        (case (:f op)
          :start
          (let [to-split (->> nodes
                              shuffle
                              (filter @statuses)
                              (take to-take)
                              (into #{}))]
            (doseq [node to-split]
              (silence! test node (cons master nodes)))
            (assoc op :value (str "Cut off " to-split)))
          :stop
          (do (net/heal! (:net test) test)
              (assoc op :value "fully connected"))))
      (teardown! [_ test]
        (future-cancel @poller)))))
