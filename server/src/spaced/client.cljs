(ns spaced.client
  (:require-macros
   [cljs.core.async.macros :as asyncm :refer (go go-loop)]
)
  (:require [rum.core :as r]
            [goog.dom :as dom]
            [cljs.core.match :refer-macros [match]]
            [impi.core :as impi]
            [cljs.core.async :as async :refer (<! >! put! chan)]
            [taoensso.sente  :as sente :refer (cb-success?)]
            [taoensso.sente.packers.transit :as sente-transit]))


(enable-console-print!)

(let [{:keys [chsk ch-recv send-fn state]}
      (sente/make-channel-socket! "/chsk" ; Note the same path as before
                                  {:type :auto ; e/o #{:auto :ajax :ws}
                                   :packer (sente-transit/get-transit-packer)
                                   })]
  (def chsk       chsk)
  (def ch-chsk    ch-recv) ; ChannelSocket's receive channel
  (def chsk-send! send-fn) ; ChannelSocket's send API fn
  (def chsk-state state)   ; Watchable, read-only atom
  )

(defn render-stage!
  [el stage-id app-state]
  (let [[w h] [2000 1000]]
    (impi/mount stage-id
                {:pixi/renderer {:pixi.renderer/size [w h]
                                 :pixi.renderer/background-color 0xbbbbbb}

                 :pixi/stage    {:impi/key :gfx
                                 :pixi.object/position [50 240]
                                 :pixi.object/type :pixi.object.type/graphics
                                 :pixi.graphics/shapes
                                 (concat

                                  (for [[_ object] (:objects app-state)]
                                    (cond (get (:role/tags (:role object)) :planet)
                                          {:pixi.shape/type     :pixi.shape.type/circle
                                           :pixi.shape/position (map / (:object/position object) (repeat 25))
                                           :pixi.circle/radius     200
                                           :pixi.shape/fill     {:pixi.fill/color 0x770000
                                                                 :pixi.fill/alpha 0.6}}

                                          (get (:role/tags (:role object)) :freighter)
                                          {:pixi.shape/type     :pixi.shape.type/rectangle
                                           :pixi.shape/position (map / (:object/position object) (repeat 25))
                                           :pixi.shape/size     [20 80]
                                           :pixi.shape/fill     {:pixi.fill/color (case (:player/id object)
                                                                                    1 0x004433
                                                                                    2 0x774433)
                                                                 :pixi.fill/alpha 0.6}}

                                          :else
                                          {:pixi.shape/type     :pixi.shape.type/rectangle
                                           :pixi.shape/position (map / (:object/position object) (repeat 25))
                                           :pixi.shape/size     [40 40]
                                           :pixi.shape/fill     {:pixi.fill/color (case (:player/id object)
                                                                                    1 0x004433
                                                                                    2 0x774433)
                                                                 :pixi.fill/alpha 0.6}}))
                                                                    (for [[_ object] (:objects app-state)
                                        :when      (get-in object [:object/behaviours :behaviour/shoot])]
                                    {:pixi.shape/type     :pixi.shape.type/circle
                                     :pixi.shape/position (map /
                                                               (get-in object [:object/behaviours :behaviour/shoot :behaviour/shoot.target :object/position])
                                                               (repeat 25))
                                     :pixi.circle/radius     100
                                     :pixi.shape/fill     {:pixi.fill/color 0x777777
                                                           :pixi.fill/alpha 0.9}}))

                                 #_ [{:pixi.shape/type :pixi.shape.type/circle
                                      :pixi.shape/position (:object/position (get (:objects app-state) 1))
                                      :pixi.circle/radius 20
                                      :pixi.shape/line
                                      {:pixi.line/width 4
                                       :pixi.line/color 0x22FF11
                                       :pixi.line/alpha 0.7}}
                                     {:pixi.shape/type :pixi.shape.type/ellipse
                                      :pixi.shape/position (:object/position (get (:objects app-state) 2))
                                      :pixi.ellipse/radius [30 20]
                                      :pixi.shape/fill {:pixi.fill/color 0xFF0000}}
                                     {:pixi.shape/type :pixi.shape.type/polygon
                                      :pixi.polygon/path [100 0, 160 0, 130 40, 100 40]
                                      :pixi.shape/fill {:pixi.fill/color 0xFFFF00}}
                                     {:pixi.shape/type :pixi.shape.type/rectangle
                                      :pixi.shape/position [170 0]
                                      :pixi.shape/size [50 40]
                                      :pixi.shape/fill {:pixi.fill/color 0x004433
                                                        :pixi.fill/alpha 0.6}}
                                     {:pixi.shape/type :pixi.shape.type/rounded-rectangle
                                      :pixi.shape/position [240 0]
                                      :pixi.shape/size [50 40]
                                      :pixi.rounded-rectangle/radius 5
                                      :pixi.shape/fill {:pixi.fill/color 0x221155}}]}}
                el)))

(def impi
  {:did-mount (fn [state]
                (render-stage! (r/dom-node state) :gameboard (first (:rum/args state)))
                state)

   :will-update (fn [state]
                  (render-stage! (r/dom-node state) :gameboard (first (:rum/args state)))
                  state)

   :will-unmount (fn [state]
                   (impi/unmount :gameboard)
                   state)})

(r/defc gameboard < impi
  [app-state]
  [:div])

(r/defc object-list
  [objects]
  [:div {:style {:width "800px" :height "100px" :right 0 :top 0 :position "absolute"}}
   [:table
    (for [[_ object] objects
          :when (not (empty? (:cargo/items object)))]
      [:tr
       [:td (:object/id object)]
;;       [:td (prn-str (get-in object [:object/behaviours :behaviour/shoot]))]
       [:td (prn-str (get-in object [:object/behaviours :behaviour/shoot :behaviour/shoot.target]))]
;;       [:td (:object/position object)]
;;       [:td (prn-str (:role object))]
       [:td (prn-str (:cargo/items object))]])]])

(r/defc root < r/reactive
  [state]
  (let [state (r/react state)]
    [:div
     (object-list (:objects state))

     (:events state)
     (gameboard state)]))

(defonce state
  (atom {:timestamp 0
         :objects   {}  ;; {1 {:object/id       1
         ;;     :object/position [0 0]
         ;;     :event/timestamp 0
         ;;     :event/movement  {:movement/start    [0 0]
         ;;                       :movement/end      [200 0]
         ;;                       :movement/duration 3000}}
         ;;  2 {:object/id       2
         ;;     :object/position [60 20]
         ;;     :event/timestamp 0
         ;;     :event/movement  {:movement/start    [60 20]
         ;;                       :movement/end      [0 0]
         ;;                       :movement/duration 3000}}}
         }))

(defn update-object-state
  [state object]
  (let [timestamp (:timestamp state)
        movement  (:event/movement object)
        dt        (- timestamp (:movement/timestamp movement))]

    (cond (not movement)
          object

          (< dt 0)
          (assoc object :object/position (:movement/start movement))

          (<= dt (:movement/duration movement))
          (let [delta-xu (map /
                              (map -
                                   (:movement/end movement)
                                   (:movement/start movement))
                              (repeat (:movement/duration movement)))
                delta-x  (map *
                              (repeat dt)
                              delta-xu)]

            (update object :object/position #(map +
                                                  (:movement/start movement)
                                                  delta-x)))

          :else
          (assoc object :object/position (:movement/end movement)))))

(defn objects-to-map
  [objects]
  (into {}
        (map (fn [object]
               [(:object/id object) object])
             objects)))

(defn process-event!
  [event]
  ;; (match event
  ;;        [:chsk/recv [:state/objects objects]]
  ;;        (swap! state update :objects merge (objects-to-map objects))

  ;;        [:chsk/recv [:state/clear {}]]
  ;;        (swap! state assoc :objects {})

  ;;        [:chsk/recv [:state/tombstone object-id]]
  ;;        (swap! state update :objects dissoc (:object/id object))

  ;;        _ (println :nop event))
  )

(defn update-state
  [state]
  (update state :objects
          (fn [objects]
            (into {}
                  (for [[id object] objects]
                    [id (update-object-state state object)])))))

(defn ^:expose main
  []
  (async/go (loop []
              (let [message (async/<! ch-chsk)]
;;                (println (:event message))
                (process-event! (:event message)))
;;              (swap! state assoc :events conj (second (first (rest (:event (async/<! ch-chsk))))))
              (recur)))

  ;; (async/go (loop []
  ;;             (swap! state update :timestamp + 100)
  ;;             (swap! state update-state)
  ;;             (async/<! (async/timeout 100))
  ;;             (recur)))

  (r/mount (root state)
           (dom/getElement "app")))
