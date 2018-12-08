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

(defmethod impi/update-prop! :pixi.event/pointer-down [object index _ listener]
  (impi/replace-listener object "pointerdown" index listener))

(defmethod impi/update-prop! :pixi.event/pointer-up [object index _ listener]
  (impi/replace-listener object "pointerup" index listener))

(defmethod impi/update-prop! :pixi.event/pointer-move [object index _ listener]
  (impi/replace-listener object "pointermove" index listener))

(defmethod impi/update-prop! :pixi.object/button-mode? [^js/PIXI.DisplayObject object _ _ button-mode?]
  (set! (.-buttonMode object) button-mode?))

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
  (let [[w h]                [2000 1000]
        {:keys [state/drag]} app-state]
    
    (impi/mount stage-id
                {:pixi/renderer {:pixi.renderer/size [w h]
                                 :pixi.renderer/background-color 0xbbbbbb}

                 :pixi/listeners {:pointer-down (fn [^js/PIXI.interaction.InteractionEvent e]
                                                  (println :pointer-down)
                                                  (let [{:keys [state/drag]} app-state
                                                        {:keys [x y]}        @drag]
                                                    (swap! drag
                                                           assoc
                                                           :mx0       (.. e -data -originalEvent -clientX)
                                                           :my0       (.. e -data -originalEvent -clientY)
                                                           :x0        x
                                                           :y0        y
                                                           :dragging? true)))
                                  
                                  :pointer-move (fn [^js/PIXI.interaction.InteractionEvent e]
                                                  (let [{:keys [state/drag]} app-state]
                                                    (when (:dragging? @drag)
                                                      (let [{:keys [x y
                                                                    mx0 my0
                                                                    x0 y0]} @drag
                                                            dmx (- mx0 (.. e -data -originalEvent -clientX))
                                                            dmy (- my0 (.. e -data -originalEvent -clientY))]
                                                        (swap! drag
                                                               assoc
                                                               :x (- x0 dmx)
                                                               :y (- y0 dmy))))))

                                  :pointer-up   (fn [_]
                                                  (let [{:keys [state/drag]} app-state]
                                                    (swap! drag assoc :dragging? false)))}
                 
                 :pixi/stage    {:impi/key                :stage
                                 :pixi.object/type        :pixi.object.type/container
                                 :pixi.object/position    [(get @drag :x 0)
                                                           (get @drag :y 0)] #_(get app-state :camera/position [0 0])

                                 :pixi.object/interactive?   true
                                 :pixi.object/contains-point (constantly true)
                                 :pixi.event/pointer-down    [:pointer-down]
                                 :pixi.event/pointer-up      [:pointer-up]
                                 :pixi.event/pointer-move    [:pointer-move]
                                 
                                 :pixi.container/children
                                 [{:impi/key                   :gfx
                                   :pixi.object/type           :pixi.object.type/graphics
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
                                                                                      2 0x774433
                                                                                      0x000000)
                                                                   :pixi.fill/alpha 0.6}}))
                                    (for [[_ object] (:objects app-state)
                                          :when      (get-in object [:object/behaviours :behaviour/shoot])]
                                      {:pixi.shape/type     :pixi.shape.type/circle
                                       :pixi.shape/position (map /
                                                                 (get-in object [:object/behaviours :behaviour/shoot :behaviour/shoot.target :object/position])
                                                                 (repeat 25))
                                       :pixi.circle/radius     100
                                       :pixi.shape/fill     {:pixi.fill/color 0x777777
                                                             :pixi.fill/alpha 0.9}}))}]}}
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
         :objects   {}
         :state/drag      (atom nil)}))

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
  (match event
         [:chsk/recv [:state/object object]]
         (swap! state assoc-in [:objects (:object/id object)] object)

         [:chsk/recv [:state/clear {}]]
         (swap! state assoc :objects {})

         [:chsk/recv [:state/tombstone object-id]]
         (swap! state update :objects dissoc object-id)

         _ nil))

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
                (process-event! (:event message)))

              (recur)))

  (r/mount (root state)
           (dom/getElement "app")))
