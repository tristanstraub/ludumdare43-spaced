(ns spaced.distributed-kd-tree
  (:require [clojure.set :as set]
            [kdtree :as kdtree]
            [spaced.simulation :as simulation]
            spaced.transit-serdes)
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams KeyValue StreamsBuilder StreamsConfig Topology$AutoOffsetReset]
           [org.apache.kafka.streams.kstream Consumed Transformer TransformerSupplier]
           [org.apache.kafka.streams.processor Processor ProcessorSupplier PunctuationType Punctuator StateRestoreCallback StateStore]
           [org.apache.kafka.streams.state StoreBuilder Stores]))

(def system-width
  5000)

(defn position-to-key
  [position]
  (mapv #(* system-width (int (/ % system-width))) position))

(defrecord MyStateStore [name context open records]
  StateStore
  (init [this ctx root]
    (reset! context ctx)
    (.register ctx root
               (reify StateRestoreCallback
                 (restore [this key value]
;;                   (println :restore key value)
                   ;;                       (.put this )
                   )
                 ))
    (reset! open true))
  (flush [_])
  (close [_])
  (isOpen [_]
    @open)
  (name [_]
    name)
  (persistent [_]
    true))

(defn state-store
  [name]
  (let [context (atom nil)
        open    (atom nil)]
    (->MyStateStore name context open (atom nil))))

(defn- to-props
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m]
      (.put ps  k v))
    ps))

(defonce ids (atom 0))
(defn id!
  []
  (swap! ids inc))

(defn app-id!
  []
  (str "test-" (id!)))

(defn stream-props
  [app-id & [{:as options}]]
  (to-props (merge {"application.id"                               app-id
                    "bootstrap.servers"                            "localhost:9092"
                    StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   spaced.transit-serdes/transit-serdes
                    StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG spaced.transit-serdes/transit-serdes
                    "commit.interval.ms" 100}
                   options)))

(defn streams
  [topology app-id & [options]]
  (KafkaStreams. topology (stream-props app-id options)))

(defn store-put-item!
  [store item]
  (swap! (:records store) kdtree/insert item))

(defn system-neighbours
  [position]
  (for [i [(- system-width) 0 system-width]
        j [(- system-width) 0 system-width]
        :when (not= [i j] [0 0])]
    (mapv + position [i j])))

(defn transformer
  [systems-store]
  (reify TransformerSupplier
    (get [this]
      (let [context  (atom nil)
            robjects (atom nil)
            systems  (atom nil)
            delta    100
            start    (atom nil)]
        (reify Transformer
          (init [this ctx]
            (reset! context ctx)
            ;;            (reset! objects (.getStateStore ctx objects-store))
            (reset! systems (.getStateStore ctx systems-store))

            (.schedule @context delta
                       PunctuationType/WALL_CLOCK_TIME
                       (reify Punctuator
                         (punctuate [_ _]
                           ;;                           (println "punctuate" :taskid (.taskId @context) :epoch @epoch)
                           (let [objects          (vals @robjects)
                                 timestamp        (+ delta (reduce max 0 (map :timestamp objects)))
                                 positions        (set (distinct (->> objects
                                                                      (remove :object/inactive?)
                                                                      (map :object/position)
                                                                      (map position-to-key))))
                                 neighbour-epochs (remove nil? (map #(.get @systems %)
                                                                    (set/difference (set (mapcat system-neighbours positions))
                                                                                    positions)))]


                             #_(println :count (count objects) :taskId (.taskId @context) :epoch @epoch ;; :neighbour-epochs neighbour-epochs
                                        )
                             (when true #_(empty? (filter #(< % @epoch) neighbour-epochs))
                                   (let [state (simulation/update-state {:timestamp timestamp
                                                                         :objects   objects
                                                                         :roles     simulation/roles})]

                                     ;;                                   (println @epoch :update neighbour-epochs)
                                     (let [object (select-keys (->> (:objects state)
                                                                    (filter (comp #{:mining-scout} :object/role))
                                                                    (sort-by :object/id)
                                                                    first)
                                                               [:object/id :object/position :object/role])]
                                       ;;                                     (println :first-object object)
                                       ;;(println :first-object object)
                                       )

                                     (reset! robjects nil)
                                     ;;                                 (println :taskid (.taskId @context) :count (count (:objects state)))


                                     (doseq [object (:objects state)
                                             :let [object-key (position-to-key (:object/position object))]]
                                       ;;                                       (println timestamp (.taskId @context) (select-keys object [:object/id :object/position :object/role :object/inactive?]))
                                       (doseq [position (conj (system-neighbours (:object/position object))
                                                              object-key)]
                                         ;;                                       (println position object-key)
                                         (.forward @context
                                                   position
                                                   (-> (if (= position object-key)
                                                         object
                                                         (assoc object :object/inactive? true))
                                                       (assoc :timestamp timestamp)))))

                                     (try
                                       (doseq [object (:tombstones state)]
                                         (.forward @context
                                                   (position-to-key (:object/position object))
                                                   (assoc object :object/inactive? true :object/tombstone? true)))
                                       (catch Exception e
                                         (println "Error while forwarding tombstones" e)))

                                     (when-let [data (seq (for [pos positions]
                                                            (KeyValue. pos timestamp)))]
                                       
                                       (.putAll @systems data)))))

                           (.commit @context)))))
          (close [this])
          (transform [this k v]
            ;;            (println :task-id (.taskId @context) :partition (.partition @context) :v v)
            ;;            (swap! (:records @store) kdtree/insert (:object/position v))
            ;;            (println :k k :v v)
            ;;            (println :object/id (:object/id v))
            (swap! robjects
                   (fn [robjects]
                     ;;                     (println :v v)
                     (if (or (not (get robjects (:object/id v)))
                             (not (:object/inactive? v)))

                       (assoc robjects (:object/id v) v)
                       robjects)))

            (.commit @context)

            nil))))))

(defn processor
  [store-name]
  (reify ProcessorSupplier
    (get [this]
      (let [context (atom nil)
            store   (atom nil)]
        (reify Processor
          (init [this ctx]
            (reset! store (.getStateStore ctx store-name))
            (reset! context ctx))
          (close [this])
          (process [this k v]
;;            (println :task-id (.taskId @context) :partition (.partition @context) :v v)
            (swap! (:records @store) kdtree/insert v)
            (.forward @context "key" (vec (kdtree/nearest-neighbor @(:records @store)
                                                                   v
                                                                   2)))))))))

(defn store-builder
  [store-name create]
  (let [logging (atom false)]
    (reify StoreBuilder
      (build [_]
        (create store-name))
      (loggingEnabled [this]
        @logging)
      (withCachingEnabled [this]
        (throw (ex-info "Not supported" {})))
      (withLoggingEnabled [this _]
        (reset! logging true)
        this)
      (withLoggingDisabled [this]
        (reset! logging false)
        this)
      (name [_]
        store-name))))

(defn topology
  []
  (let [builder (StreamsBuilder.)]
    (.addStateStore builder
                    (store-builder "kdtree" state-store))

    ;; (.addStateStore builder
    ;;                 ;;(StoreBuilder )
    ;;                 (Stores/keyValueStoreBuilder
    ;;                  (Stores/persistentKeyValueStore "objects")
    ;;                  (spaced.transit-serdes/transit-serdes*)
    ;;                  (spaced.transit-serdes/transit-serdes*)))

    (.addGlobalStore builder
                     (Stores/keyValueStoreBuilder
                      (Stores/persistentKeyValueStore "systems")
                      (spaced.transit-serdes/transit-serdes*)
                      (spaced.transit-serdes/transit-serdes*))
                     "systems"
                     (Consumed/with Topology$AutoOffsetReset/LATEST)
                     (reify ProcessorSupplier
                       (get [this]
                         (let [context (atom nil)
                               store   (atom nil)]
                           (reify Processor
                             (init [this ctx]
                               (reset! store (.getStateStore ctx "systems"))
                               (reset! context ctx))
                             (close [this])
                             (process [this k v]
                               (.put @store k v)))))))

    (-> builder
        (.stream "objects")
        (.transform (transformer "systems")
                    (into-array String []))
        (.to "objects"))

    (.build builder)))

(defn produce!
  [p topic k v]
  (.send p (ProducerRecord. topic k v)))

;; (def c (KafkaConsumer. (to-props {"bootstrap.servers"  "localhost:9092"
;;                                   "group.id"           "test-3"
;;                                   "key.deserializer"   spaced.transit-serdes/transit-deserializer
;;                                   "value.deserializer" spaced.transit-serdes/transit-deserializer})))
;; (.subscribe c ["output-topic"])

;; (def rs (.poll c 100))

(defonce p (KafkaProducer. (to-props {"bootstrap.servers" "localhost:9092"
                                      "key.serializer" spaced.transit-serdes/transit-serializer
                                      "value.serializer" spaced.transit-serdes/transit-serializer})))

(comment
  (let [t (topology)
        id (app-id!)]
    (def s1 (streams t id {"state.dir" "kafka-streams/test-1"}))
    (def s2 (streams t id {"state.dir" "kafka-streams/test-2"}))
    (def s3 (streams t id {"state.dir" "kafka-streams/test-3"})))

  (do
    (.start s1)
    (.start s2)
    (.start s3))

  (do
    (.close s1)
    (.close s2)
    (.close s3))

  )



(defn metrics
  [s]
  (into {} (map (fn [[k v]] [(.name k) (.value v)]) (into {} (.metrics s)))))

(comment
  (doseq [object (simulation/initial-objects) #_(simulation/test-objects)
          :let [object-key (position-to-key (:object/position object))]
          position (conj (system-neighbours (:object/position object))
                         object-key)]
    @(produce! p
               "objects"
               (position-to-key position)
               (-> (if (= position object-key)
                     object
                     (assoc object :object/inactive? true))
                   (assoc :timestamp 0))))

  (doseq [i (range 0 100 10)
          j (range 0 100 10)
          :let [object {:object/id       (+ (* i 100) j)
                        :object/position [i j]}
                ]]
    ))

#_(.start s3)

#_(with-open [driver (TopologyTestDriver. (topology) (stream-props))]
  (let [records (ConsumerRecordFactory. "investors"
                                        (spaced.transit-serdes/transit-serializer*)
                                        (spaced.transit-serdes/transit-serializer*))]
    (.. driver (pipeInput (.. records (create "objects" "key" [0 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [1 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [10 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [9.5 0]))))

    (dotimes [i 10]
      (if-let [event (.. driver (readOutput "output-topic"
                                            (spaced.transit-serdes/transit-deserializer*)
                                            (spaced.transit-serdes/transit-deserializer*)))]
        (println (.value event))))))

(defn objects-consumer
  []
  (let [c (KafkaConsumer. (to-props {"bootstrap.servers"  "localhost:9092"
                                     "group.id"           (str "test-4-consumer-" (rand-int 10000))
                                     "key.deserializer"   spaced.transit-serdes/transit-deserializer
                                     "value.deserializer" spaced.transit-serdes/transit-deserializer}))]
    (.subscribe c ["objects"])

    c))

(defn objects
  [c]
  (->> (iterator-seq (.iterator (.poll c 1000)))
       (map #(.value %))
       (remove #(and (:object/inactive? %)
                     (not (:object/tombstone? %))))
;;       (filter #(= [0 0] (position-to-key (:object/position %))))
       (group-by :object/id)
       (map (fn [[id objects]]
              (let [tombstones (filter :object/tombstone? objects)]
                (if (seq tombstones)
                  (first tombstones)
                  (first (sort-by :timestamp > objects))))))))

