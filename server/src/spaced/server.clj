(ns spaced.server
  (:gen-class)
  (:require [aleph.http :as http]
            [reitit.ring :as ring]
            ring.middleware.keyword-params
            ring.middleware.params
            [spaced.distributed-kd-tree :as distributed-kd-tree]
            [spaced.simulation :as sim]
            [taoensso.sente :as sente]
            [taoensso.sente.packers.transit :as sente-transit]
            [taoensso.sente.server-adapters.aleph :refer [get-sch-adapter]]
            [spaced.simulation :as simulation])
  (:import java.util.Properties
           org.apache.kafka.clients.admin.KafkaAdminClient))

(defn init-ws!
  []
  (let [{:keys [ch-recv send-fn connected-uids
                ajax-post-fn ajax-get-or-ws-handshake-fn]}
        (sente/make-channel-socket! (get-sch-adapter)
                                    {:user-id-fn (fn [ring-req] (:client-id ring-req))
                                     :packer     (sente-transit/get-transit-packer)})]


    (def ring-ajax-post                ajax-post-fn)
    (def ring-ajax-get-or-ws-handshake ajax-get-or-ws-handshake-fn)
    (def ch-chsk                       ch-recv) ; ChannelSocket's receive channel
    (def chsk-send!                    send-fn) ; ChannelSocket's send API fn
    (def connected-uids                connected-uids) ; Watchable, read-only atom
    ))

(defn app
  []
  (-> (ring/ring-handler
       (ring/router
        ["/chsk" {:get {:handler ring-ajax-get-or-ws-handshake}
                  :post {:handler ring-ajax-post}}]))
      ring.middleware.keyword-params/wrap-keyword-params
      ring.middleware.params/wrap-params
      ;;(constantly {:status 404})
      ;; (cors/wrap-cors :access-control-allow-origin [#"http://localhost:9500"]
      ;;                 :access-control-allow-methods [:get :put :post :delete])
      ))

;; (defn handler [req]
;;   {:status 200
;;    :headers {"content-type" "text/plain"}
;;    :body "hello!!"})

(defonce s (atom nil))



(defn update-object-store
  [objects current]
  (reduce (fn [objects object]
            (cond (:object/tombstone? (get objects (:object/id object)))
                  objects

                  (:object/tombstone? object)
                  (assoc objects (:object/id object) object)
                  
                  (or (not (get objects (:object/id object)))
                      (> (:timestamp object)
                         (:timestamp (get objects (:object/id object)))))
                  (assoc objects (:object/id object) object)
                  
                  :else
                  objects))
          objects
          current))

(defn simulate!
  []
  (doto (Thread. (fn []
                   (with-open [consumer (distributed-kd-tree/objects-consumer)]
                     (try

                       (sim/init!)
                       (when (first (:ws @connected-uids))
                         (doseq [client (:ws @connected-uids)]
                           (chsk-send! (first (:ws @connected-uids)) [:state/clear {}])))

                       (loop [objects {}]
                         (let [previous (:objects @sim/state)
                               current  (distributed-kd-tree/objects consumer)
                               ;; (:objects (sim/timestep! 100))
                               objects (update-object-store objects current)]

                           (when (first (:ws @connected-uids))
                             (chsk-send! (first (:ws @connected-uids))
                                         [:state/objects
                                          (map (fn [object]
                                                 (assoc (select-keys object
                                                                     [:player/id
                                                                      :object/id :object/position :cargo/items :object/behaviours :object/parts])
                                                        :role (sim/find-object-role @sim/state object)))
                                               (remove :object/tombstone? (vals objects)))]))

                           #_ (doseq [ ;; client (:ws @connected-uids)
                                      object current]
                                (when (sim/planet?  @sim/state object)
                                  (println :planet? (sim/planet? @sim/state object) (:object/id object)))
                                (chsk-send! (first (:ws @connected-uids)) [:state/object
                                                                           (assoc (select-keys object
                                                                                               [:player/id
                                                                                                :object/id :object/position :cargo/items :object/behaviours])
                                                                                  :role (sim/find-object-role @sim/state object))]
                                            ;;                           {:flush? true}
                                            ))

                           (when (first (:ws @connected-uids))
                             (doseq [ ;; client (:ws @connected-uids)
                                     object-id (map :object/id (filter :object/tombstone? current))]

                               (println :send-tombstone object-id)
                               (chsk-send! (first (:ws @connected-uids)) [:state/tombstone object-id]))))
                         (recur objects))
                       (catch InterruptedException e
                         (println e))))))
    (.start)))

(defonce simulation (atom nil))

(defn- to-props
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m]
      (.put ps  k v))
    ps))

(defn serve!
  []
  (init-ws!)
  ;;  {:pre [(not @s)]}
  (reset! s (http/start-server (app)
                               {:port 8080})))

(defn -main
  [& argv]
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (println ex "Uncaught exception on" (.getName thread)))))
  
  (serve!)

  (reset! simulation (simulate!))

  #_(.stop @simulation)

  (when (= "--seed" (nth argv 0))
    (.deleteTopics (KafkaAdminClient/create (to-props {"bootstrap.servers" "localhost:9092"}))
                   ["objects"])
    (distributed-kd-tree/seed!)

    )
  
  (distributed-kd-tree/run-world!)
  #_(distributed-kd-tree/stop-world!)
  ;;  (.close @s)

  (println "Server started on 8080")
  )

#_@f
#_ (future-cancel f)
