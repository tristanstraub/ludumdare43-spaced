(ns spaced.server
  (:require [spaced.simulation :as sim]
            [aleph.http :as http]
            [reitit.ring :as ring]
            [taoensso.sente :as sente]
            [taoensso.sente.server-adapters.aleph :refer (get-sch-adapter)]
            [ring.middleware.cors :as cors :refer [wrap-cors]]
            [clojure.core.async :as async]
            [ring.middleware.keyword-params]
            [ring.middleware.params]))

(let [{:keys [ch-recv send-fn connected-uids
              ajax-post-fn ajax-get-or-ws-handshake-fn]}
      (sente/make-channel-socket! (get-sch-adapter)
                                  {:user-id-fn (fn [ring-req] (:client-id ring-req))})]


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
  {:pre [(not @s)]}
  (reset! s (http/start-server #'app
                               {:port 8080}))
  ;;  (.close @s)
  )

#_(def f (future (sim/init!)
                 (doseq [client (:ws @connected-uids)]
                   (chsk-send! (first (:ws @connected-uids)) [:state/clear {}]))

                 (while true
                   (Thread/sleep 100)
                   (sim/timestep! 100)

                   (doseq [client (:ws @connected-uids)
                           object (:objects @sim/state)]

                     (chsk-send! (first (:ws @connected-uids)) [:state/object (assoc (select-keys object
                                                                                                  [:player/id
                                                                                                   :object/id :object/position :cargo/items :object/behaviours])
                                                                                     :role (sim/find-object-role @sim/state object))])))))
#_@f
#_ (future-cancel f)
