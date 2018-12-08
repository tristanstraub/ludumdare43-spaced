(ns spaced.server
  (:require [aleph.http :as http]
            [clojure.set :as set]
            [reitit.ring :as ring]
            ring.middleware.keyword-params
            ring.middleware.params
            [taoensso.sente :as sente]
            [taoensso.sente.server-adapters.aleph :refer [get-sch-adapter]]
            [taoensso.sente.packers.transit :as sente-transit]
            [spaced.simulation :as sim]))

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
  )

(def app
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

(defn main []
;;  {:pre [(not @s)]}
  (reset! s (http/start-server #'app
                               {:port 8080}))
  ;;  (.close @s)
  )

(defn simulate!
  []
  (doto (Thread. (fn []
                   (try
                     (sim/init!)
                     (doseq [client (:ws @connected-uids)]
                       (chsk-send! (first (:ws @connected-uids)) [:state/clear {}]))

                     (loop []
                       (Thread/sleep 10)
                       (let [previous (:objects @sim/state)
                             current  (:objects (sim/timestep! 100))]

                         (doseq [ ;; client (:ws @connected-uids)
                                 object current]

                           (chsk-send! (first (:ws @connected-uids)) [:state/object
                                                                      (assoc (select-keys object
                                                                                          [:player/id
                                                                                           :object/id :object/position :cargo/items :object/behaviours])
                                                                             :role (sim/find-object-role @sim/state object))]
                                       ;;                           {:flush? true}
                                       ))

                         (doseq [ ;; client (:ws @connected-uids)
                                 object-id (set/difference (set (map :object/id previous))
                                                           (set (map :object/id current)))]

                           (chsk-send! (first (:ws @connected-uids)) [:state/tombstone object-id])))
                       (recur))
                     (catch InterruptedException e
                       (println e)))))
    (.start)))

#_@f
#_ (future-cancel f)
