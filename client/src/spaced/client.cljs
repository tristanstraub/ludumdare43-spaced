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

(defmethod impi/update-prop! :pixi.object/zorder [^js/PIXI.DisplayObject object _ _ z-order]
  (set! (.-zOrder object) z-order))

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

(defn brighten
  [color]
  
  (+ color 0x606060))

(defonce stars
  (for [i (range 3000)]
    {:pixi.shape/type     :pixi.shape.type/rectangle
     :pixi.shape/position [(rand-int 200000)
                           (rand-int 200000)]
     :pixi.shape/size     [40 40]
     :pixi.shape/fill     {:pixi.fill/color 0xffffff
                           :pixi.fill/alpha 1}}))

(defn render-stage!
  [el stage-id app-state]
  (let [[w h]                [(:width app-state)
                              (:height app-state)]
        {:keys [state/drag state/scroll]} app-state]

    (println :planets (count (filter (fn [[_ object]] (get (:role/tags (:role object)) :planet))
                                     (:objects app-state))))
    
    (impi/mount stage-id
                {:pixi/renderer {:pixi.renderer/size [w h]
                                 :pixi.renderer/background-color 0x000000}

                 :pixi/listeners {:pointer-down (fn [^js/PIXI.interaction.InteractionEvent e]
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

                 :pixi/stage    {:impi/key                   :stage
                                 :pixi.object/type           :pixi.object.type/container
                                 :pixi.object/position       [(get @drag :x 0)
                                                              (get @drag :y 0)]

                                 :pixi.object/scale          [(get @drag :scale-x 1)
                                                              (get @drag :scale-y 1)]

                                 :pixi.object/interactive?   true
                                 :pixi.object/contains-point (constantly true)
                                 :pixi.event/pointer-down    [:pointer-down]
                                 :pixi.event/pointer-up      [:pointer-up]
                                 :pixi.event/pointer-move    [:pointer-move]

                                 :pixi.container/children
                                 [ {:impi/key             :stars
                                   :pixi.object/type     :pixi.object.type/graphics
                                   :pixi.graphics/shapes stars}

                                  {:impi/key                   :gfx
                                   :pixi.object/type           :pixi.object.type/graphics
                                   :pixi.graphics/shapes
                                   (concat

                                    
                                    (for [[_ object] (:objects app-state)]
                                      (cond (get (:role/tags (:role object)) :planet)
                                            {:pixi.shape/type     :pixi.shape.type/circle
                                             :pixi.shape/position (:object/position object)
                                             :pixi.circle/radius     3000
                                             :pixi.shape/fill     {:pixi.fill/color (brighten 0x770000)
                                                                   :pixi.fill/alpha 1}
                                             :pixi.object/zorder  -100}

                                            (get (:role/tags (:role object)) :freighter)
                                            {:pixi.shape/type     :pixi.shape.type/rectangle
                                             :pixi.shape/position (:object/position object) 
                                             :pixi.shape/size     [80 320]
                                             :pixi.shape/fill     {:pixi.fill/color (case (:player/id object)
                                                                                      1 (brighten 0x004499)
                                                                                      2 (brighten 0x774499))
                                                                   :pixi.fill/alpha 1}}

                                            :else
                                            {:pixi.shape/type     :pixi.shape.type/rectangle
                                             :pixi.shape/position (:object/position object) 
                                             :pixi.shape/size     [80 80]
                                             :pixi.shape/fill     {:pixi.fill/color (case (:player/id object)
                                                                                      1 (brighten 0x004433)
                                                                                      2 (brighten 0x774433)
                                                                                      0x000000)
                                                                   :pixi.fill/alpha 1}
                                             :pixi.object/zorder 100}))
                                    (for [[_ object] (:objects app-state)
                                          :when      (get-in object [:object/behaviours :behaviour/shoot])]
                                      {:pixi.shape/type     :pixi.shape.type/circle
                                       :pixi.shape/position (get-in object [:object/behaviours :behaviour/shoot :behaviour/shoot.target :object/position])
                                       
                                       :pixi.circle/radius     100
                                       :pixi.shape/fill     {:pixi.fill/color 0x777777
                                                             :pixi.fill/alpha 1}}))}]}}
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
;;    [:table
;;     (for [[_ object] objects
;;           :when (not (empty? (:cargo/items object)))]
;;       [:tr
;;        [:td (:object/id object)]
;; ;;       [:td (prn-str (get-in object [:object/behaviours :behaviour/shoot]))]
;;        [:td (prn-str (get-in object [:object/behaviours :behaviour/shoot :behaviour/shoot.target]))]
;; ;;       [:td (:object/position object)]
;; ;;       [:td (prn-str (:role object))]
;;        [:td (prn-str (:cargo/items object))]])]
   ])

(r/defc root < r/reactive
  [state]
  (let [state (r/react state)]
    [:div
     (object-list (:objects state))

     (:events state)
     (gameboard state)]))

(defonce state
  (atom {:timestamp    0
         :objects      {}
         :state/scroll 1
         :state/drag   (atom {:x 0
                              :y 0
                              :scale-x 0.02
                              :scale-y 0.02})}))

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

         [:chsk/recv [:state/objects objects]]
         (swap! state (fn [state] (update state :objects merge (objects-to-map objects))))

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

(defn zoom
  [old-pos old-scale s x y]
  (let [s              (if (> s 0) 1.1 0.9)
        world-pos      [(/ (- x (old-pos 0)) (old-scale 0))
                        (/ (- y (old-pos 1)) (old-scale 1))]
        new-scale      [(* s (old-scale 0))
                        (* s (old-scale 1))]
        new-screen-pos [(+ (old-pos 0) (* (world-pos 0) (new-scale 0)))
                        (+ (old-pos 1) (* (world-pos 1) (new-scale 1)))]

        new-stage-pos  [(- (old-pos 0) (- (new-screen-pos 0) x))
                        (- (old-pos 1) (- (new-screen-pos 1) y))]]
    [new-stage-pos new-scale]))


(defn ^:expose main
  []
  (let [size (dom/getViewportSize)
        w (.-width size)
        h (.-height size) ]

    (swap! state assoc :width w :height h)
    
    (async/go (loop []
                (let [message (async/<! ch-chsk)]
                  (process-event! (:event message)))

                (recur)))

    (.addEventListener (dom/getElement "app")
                       "mousewheel"
                       (fn [^js/WheelEvent e]
                         (.preventDefault e)

                         (let [[new-stage-pos new-scale] (zoom [(get @(:state/drag @state) :x 0)
                                                                (get @(:state/drag @state) :y 0)]
                                                               [(get @(:state/drag @state) :scale-x 1)
                                                                (get @(:state/drag @state) :scale-y 1)]
                                                               (- (.. e -deltaY)) (.. e -offsetX) (.. e -offsetY))]
                           (swap! (:state/drag @state) (fn [drag]
                                                         (assoc drag
                                                                :x (new-stage-pos 0)
                                                                :y (new-stage-pos 1)
                                                                :scale-x (new-scale 0)
                                                                :scale-y (new-scale 1)))))))

    (r/mount (root state)
             (dom/getElement "app"))))
