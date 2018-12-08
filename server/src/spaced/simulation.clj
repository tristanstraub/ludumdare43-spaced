(ns spaced.simulation
  (:require [kdtree :as kdtree]))

(defonce state (atom {}))

(defonce ids (atom 0))

(defn id!
  [object]
  (assoc object :object/id (swap! ids inc)))

(defn object!
  [options]
  (id! (merge {:object/position   [0 0]
               :shooting/range    -1
               :mining/range      -1
               :mining/speed      0
               :collect/range     -1
               :transport/range   -1
               :cargo/capacity    0
               :cargo/items       {}
               :cargo/used        0
               :object/behaviours {}
               :object/speed      (/ 3000 1000)}
              options)))

(def role-mining-scout
  {:role/rules {:within-shooting-range                  :shoot
                :cargo-not-full-and-within-mining-range :mine
                :cargo-full-and-nearby-freighter        :move-to-freighter
                :cargo-empty-and-away                   :move-to-planet}
   :role/tags #{:hauler}})

(def role-freighter
  {:role/tags #{:freighter}
   :role/rules {:hauler-nearby-and-has-cargo :collect-cargo}})

(def role-planet
  {:role/tags  #{:planet}})


(defn add-to-cargo
  [object material amount]
  (-> object
      (update-in [:cargo/items material] (fnil + 0) amount)
      (update :cargo/used (fnil + 0) amount)))

(defn remove-from-cargo
  [object material amount]
  (-> object
      (update-in [:cargo/items material] (fnil - 0) amount)
      (update :cargo/used (fnil - 0) amount)))

(defn distance
  [a b]
  (let [d (map - a b)]
    (Math/sqrt (reduce + (map * d d)))))

(defn find-object-role
  [state object]
  (get (:roles state) (:object/role object)))

(defn within-shooting-range?
  [object target]
  (< (distance (:object/position object)
               (:object/position target))
     (:shooting/range object)))

(defn within-mining-range?
  [object target]
  (< (distance (:object/position object)
               (:object/position target))
     (:mining/range object)))

(defn within-transport-range?
  [object target]
  true
#_  (< (distance (:object/position object)
               (:object/position target))
     (:transport/range object)))

(defn within-collect-range?
  [object target]
  (< (distance (:object/position object)
               (:object/position target))
     (:collect/range object)))

(defn cargo-full?
  [object]
  (= (:cargo/used object)
     (:cargo/capacity object)))

(defn cargo-empty?
  [object]
  (= (:cargo/used object)
     0))

(defn not-self
  [object coll]
  (filter #(not= (:object/id object)
                 (:object/id %))
          coll))

(defn sort-by-distance
  [object coll]
  (sort-by #(distance (:object/position object)
                      (:object/position %))
           coll))

(defn same-player?
  [object target]
  (= (:player/id object)
     (:player/id target)))


(defn has-role-tag?
  [state object tag]
  (get (:role/tags (find-object-role state object))
       tag))

(defn planet?
  [state object]
  (has-role-tag? state object :planet))

(defn hauler?
  [state object]
  (has-role-tag? state object :hauler))

(defn freighter?
  [state object]
  (has-role-tag? state object :freighter))

(defn add-to-tree
  [state tree object]
  (cond-> tree
    (not (planet? state object)) (update :shooting kdtree/insert (with-meta (:object/position object) {:object object}))
    (planet? state object) (update :mining kdtree/insert (with-meta (:object/position object) {:object object}))
    (freighter? state object) (update :freighter kdtree/insert (with-meta (:object/position object) {:object object}))
    (hauler? state object) (update :hauler kdtree/insert (with-meta (:object/position object) {:object object}))))

(defn nearest
  [object targets key]
  (map (fn [v] (-> v meta :object))
       (kdtree/nearest-neighbor (get targets key)
                                (:object/position object)
                                10)))

(defn nearest-shooting-target
  [state object targets]
  (first (filter #(and (within-shooting-range? object %)
                       (not (same-player? object %)))
                 (sort-by-distance object (not-self object (nearest object targets :shooting))))))

(defn nearest-mining-target
  [state object targets]
  (first (filter #(and (within-mining-range? object %))
                 (not-self object (nearest object targets :mining)))))

(defn nearest-remote-mining-target
  [state object targets]
  (first (filter #(and (within-transport-range? object %))
                 (not-self object (nearest object targets :mining)))))

(defn nearest-freighter
  [state object targets]
  (first (filter #(and (within-transport-range? object %)
                       (same-player? object %))
                 (sort-by-distance object (not-self object (nearest object targets :freighter))))))

(defn nearest-hauler
  [state object targets]
  (first (filter #(and (within-collect-range? object %)
                       (same-player? object %))
                 (sort-by-distance object (not-self object (nearest object targets :hauler))))))

(defn condition-applicable?
  [state object condition targets]
  (case condition
    :within-shooting-range                  (nearest-shooting-target state object targets)
    :cargo-not-full-and-within-mining-range (and (nearest-mining-target state object targets)
                                                 (not (cargo-full? object)))
    :cargo-full-and-nearby-freighter        (and (cargo-full? object)
                                                 (nearest-freighter state object targets))
    :cargo-empty-and-away                   (and (cargo-empty? object)
                                                 (nearest-remote-mining-target state object targets)
                                                 (not (nearest-mining-target state object targets)))
    :hauler-nearby-and-has-cargo            (if-let [hauler (nearest-hauler state object targets)]
                                              (not (cargo-empty? hauler)))))

(defn mark-applied-timestamp
  [state object behaviour-key]
  (assoc-in object [:object/behaviours behaviour-key :behaviour/applied.timestamp] (:timestamp state)))

(defn move-object
  [state object]
  (let [timestamp (:timestamp state)
        movement  (get-in object [:object/behaviours :behaviour/movement])
        dt        (- timestamp (:behaviour/timestamp movement))
        duration  (:behaviour/movement.duration movement)]

    (cond (< dt 0)
          (assoc object :object/position (:behaviour/movement.start movement))

          (<= dt duration)
          (let [delta-xu (map /
                              (map -
                                   (:behaviour/movement.end movement)
                                   (:behaviour/movement.start movement))
                              (repeat duration))
                delta-x  (map *
                              (repeat dt)
                              delta-xu)]

            (assoc object :object/position (map +
                                                (:behaviour/movement.start movement)
                                                delta-x)))

          :else
          (assoc object :object/position (:behaviour/movement.end movement)))))

;; TODO remove transport behaviour when cargo is empty and transport was on (or original trigger is no longer true)

(def milliseconds 1000)

(defn mine
  [state object behaviour-value]
  (let [delta     (- (:timestamp state)
                     (or (:behaviour/applied.timestamp behaviour-value)
                         (:behaviour/timestamp behaviour-value)))
        remaining (- (:cargo/capacity object)
                     (:cargo/used object))
        mined     (min remaining
                       (* (:mining/speed object) delta))]
    (add-to-cargo object :copper mined)))

(defn apply-behaviour-shoot
  [state object behaviour-key]
  (let [behaviour-value (get-in object [:object/behaviours behaviour-key])]
    ;; (as-> object object
    ;;   (shoot state object behaviour-value)
    ;;   (mark-applied-timestamp state object behaviour-key))
    object
    ))

(defn apply-behaviour-mine
  [state object behaviour-key]
  (let [behaviour-value (get-in object [:object/behaviours behaviour-key])]
    (as-> object object
      (mine state object behaviour-value)
      (mark-applied-timestamp state object behaviour-key))))

(defn apply-behaviour-movement
  [state object behaviour-key]
  (let [behaviour-value (get-in object [:object/behaviours behaviour-key])]
    (if (= (:timestamp state) (:behaviour/applied.timestamp behaviour-value))
      object
      (as-> object object
        (mark-applied-timestamp state object behaviour-key)
        (move-object state object)))))

(defn apply-behaviour-transport
  [state object behaviour-key]
  (let [behaviour-value (get-in object [:object/behaviours behaviour-key])]
    (if (:behaviour/applied.timestamp behaviour-value)
      object
      (let [target   (:behaviour/transport.target behaviour-value)
            movement {:behaviour/timestamp         (:timestamp state)
                      :behaviour/movement.duration (/ (distance (:object/position object)
                                                                (:object/position target))
                                                      (:object/speed object))
                      :behaviour/movement.start    (:object/position object)
                      :behaviour/movement.end      (:object/position target)}]
        (as-> object object
          (mark-applied-timestamp state object behaviour-key)
          (assoc-in object [:object/behaviours :behaviour/movement] movement)
          (apply-behaviour-movement state object :behaviour/movement))))))

(defn apply-object-behaviours
  [state object]
  (reduce (fn [object behaviour-key]
            (case behaviour-key
              :behaviour/transport (apply-behaviour-transport state object behaviour-key)
              :behaviour/movement  (apply-behaviour-movement state object behaviour-key)
              :behaviour/mine      (apply-behaviour-mine state object behaviour-key)
              :behaviour/shoot     (apply-behaviour-shoot state object behaviour-key)))
          object
          (keys (:object/behaviours object))))

(defn conditions
  [state object role targets]
  (filter #(condition-applicable? state object (first %) targets) (:role/rules role)))

(defn disable-local-action
  [state object action]
  (case action
    :shoot             (update object :object/behaviours dissoc :behaviour/shoot)
    :mine              (update object :object/behaviours dissoc :behaviour/mine)
    :move-to-freighter (if (= (get-in object [:object/behaviours :behaviour/transport :behaviour/action])
                              action)
                         (update object :object/behaviours dissoc :behaviour/transport)
                         object)
    :move-to-planet    (if (= (get-in object [:object/behaviours :behaviour/transport :behaviour/action])
                              action)
                         (update object :object/behaviours dissoc :behaviour/transport)
                         object)
    :collect-cargo     object))

(defn apply-local-action
  [state object action targets]
;;  (println :timestamp (:timestamp state) :object/id (:object/id object) :role (:object/role object) :action action)
  (case action
    :shoot             (assoc-in object
                                 [:object/behaviours :behaviour/shoot]
                                 {:behaviour/timestamp    (:timestamp state)
                                  :behaviour/shoot.target (nearest-shooting-target state object targets)
                                  :behaviour/condition    action})

    :mine              (if (get-in object [:object/behaviours :behaviour/mine])
                         object
                         (assoc-in object
                                   [:object/behaviours :behaviour/mine]
                                   {:behaviour/timestamp   (:timestamp state)
                                    :behaviour/mine.target (nearest-mining-target state object targets)
                                    :behaviour/condition      action}))

    :move-to-freighter (if (= action (get-in object [:object/behaviours :behaviour/transport :behaviour/condition]))
                         object
                         (if-let [target (nearest-freighter state object targets)]
                           (assoc-in object
                                     [:object/behaviours :behaviour/transport]
                                     {:behaviour/timestamp        (:timestamp state)
                                      :behaviour/transport.target target
                                      :behaviour/condition           action})
                           object))

    :move-to-planet    (if (and (get-in object [:object/behaviours :behaviour/transport])
                                (= :move-to-planet (get-in object [:object/behaviours :behaviour/transport :behaviour/condition])))
                         object
                         (if-let [target (nearest-remote-mining-target state object targets)]
                           (assoc-in object
                                     [:object/behaviours :behaviour/transport]
                                     {:behaviour/timestamp        (:timestamp state)
                                      :behaviour/transport.target target
                                      :behaviour/condition           action})
                           object))

    :collect-cargo     object))

(defn replace-object
  [state object]
  (update state :objects
          (fn [objects]
            (map #(if (= (:object/id object) (:object/id %))
                    object
                    %)
                 objects))))

(defn remove-object
  [state object]
  (update state :objects
          (fn [objects]
            (doall (remove #(= (:object/id object) (:object/id %))
                           objects)))))

(defn replace-objects
  [state targets]
  (reduce replace-object state targets))

(defn apply-global-action
  [state object action targets]
  (replace-objects state (case action
                           :collect-cargo (if-let [hauler (nearest-hauler state object targets)]
                                            (let [amount (get-in hauler [:cargo/items :copper])]
                                              [(add-to-cargo object :copper amount)
                                               (remove-from-cargo hauler :copper amount)])
                                            [])
                           [])))

(defn apply-global-object-behaviour
  [state object behaviour-key behaviour-value]
  (case behaviour-key
    :behaviour/shoot (remove-object state (:behaviour/shoot.target behaviour-value))
    state))

(defn apply-object-role
  [state object role targets]
  (let [applicable-conditions (set (keys (conditions state object role targets)))]
    (reduce (fn [object [condition action]]
              (if (get applicable-conditions condition)
                (apply-local-action state object action targets)
                (disable-local-action state object action)))
            object
            (:role/rules role))))

(defn apply-global-actions
  ([state targets]
   (reduce (partial apply-global-actions targets) state (:objects state)))
  ([targets state object]
   (reduce (fn [state [condition action]]
             (apply-global-action state object action targets))
           state
           (conditions state object (find-object-role state object) targets))))

(defn apply-global-object-behaviours
  ([state]
   (reduce apply-global-object-behaviours state (:objects state)))
  ([state object]
   (reduce (fn [state [behaviour-key behaviour-value]]
             (apply-global-object-behaviour state object behaviour-key behaviour-value))
           state
           (:object/behaviours object))))

(defn objects-kd-tree
  [state objects]
  (reduce (fn [tree object]
            (add-to-tree state tree object))
          nil
          objects))

(defn update-state
  [state]
  (let [targets (objects-kd-tree state (:objects state))]
    (-> state
        (update :objects #(for [object %]
                            (as-> object object
                              (apply-object-role state object (find-object-role state object) targets)
                              (apply-object-behaviours state object))))
        (apply-global-actions targets)
        apply-global-object-behaviours)))

(defn init!
  []
  (reset! state (update-state {:timestamp 0
                               :objects   (concat (repeatedly 5 #(object! {:object/position [(rand-int 50000)
                                                                                              (rand-int 50000)]
                                                                            :object/role     :planet}))
                                                  (repeatedly 400 #(object! {:player/id 1
                                                                            :object/position [(rand-int 50000)
                                                                                              (rand-int 50000)]
                                                                            :transport/range 2000
                                                                            :mining/range    50
                                                                            :object/role     :mining-scout
                                                                            :shooting/range  3000
                                                                            :cargo/capacity  1000
                                                                            :mining/speed    1}))

                                                  (repeatedly 10 #(object! {:player/id 1
                                                                            :object/position [(rand-int 50000)
                                                                                              (rand-int 50000)]
                                                                            :object/role     :freighter
                                                                            :collect/range   50}))

                                                  (repeatedly 400 #(object! {:player/id 2
                                                                            :object/position [(rand-int 50000)
                                                                                              (rand-int 50000)]
                                                                            :transport/range 2000
                                                                            :mining/range    50
                                                                            :object/role     :mining-scout
                                                                            :cargo/capacity  1000
                                                                            :mining/speed    1}))

                                                  (repeatedly 10 #(object! {:player/id 2
                                                                            :object/position [(rand-int 50000)
                                                                                              (rand-int 50000)]
                                                                            :object/role     :freighter
                                                                            :collect/range   50})))
                               :roles     {:mining-scout role-mining-scout
                                           :freighter    role-freighter
                                           :planet       role-planet}})))

(defn timestep!
  [delta]
  (swap! state (fn [state]
                 (-> state
                     (update :timestamp + delta)
                     update-state))))

(comment
  (init!)
  (swap! state update-state)
  (timestep! 1000)
  (swap! state update-state)

  (->> (apply-local-action @state (first (:objects @state)) :move-to-freighter)
       (apply-object-behaviours @state))

  (->> (apply-object-role @state (first (:objects @state)) role-mining-scout)
       (apply-object-behaviours @state))

  (do (init!)
      (dotimes [i 20] (timestep! 1000))
      (map #(select-keys % [:object/position :object/role :cargo/items]) (:objects @state))))
