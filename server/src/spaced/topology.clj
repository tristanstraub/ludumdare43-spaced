(ns spaced.topology
  (:require spaced.transit-serdes)
  (:import java.util.Properties
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig TopologyTestDriver]
           (org.apache.kafka.streams.kstream Transformer TransformerSupplier)
           [org.apache.kafka.streams.processor StateRestoreCallback StateStore]
           org.apache.kafka.streams.state.StoreBuilder
           org.apache.kafka.streams.test.ConsumerRecordFactory))

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
    (->MyStateStore name context open (atom []))))

;;(state-store {:keys ["kd-tree-1"]})

(defn setup!
  []
  (let [props (Properties.)
        numPartitions 3
        replicationFactor 1]
    (.. props (setProperty AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
    (let [client (.. AdminClient (create props))]
      (.. client (createTopics [(NewTopic. "objects"
                                           numPartitions
                                           replicationFactor)])))))


(defn- to-props
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m]
      (.put ps  k v))
    ps))

(defn produce!
  []
  (let [p (KafkaProducer. (to-props {"bootstrap.servers" "localhost:9092"
                                     "key.serializer" spaced.transit-serdes/transit-serializer
                                     "value.serializer" spaced.transit-serdes/transit-serializer}))]
    (.send p (ProducerRecord. "input-topic" "key" "value"))
    (.close p)))

(defonce ids (atom 0))
(defn id!
  []
  (swap! ids inc))

(defn stream-props
  []
  (to-props {"application.id"                               (str "test-" (id!))
             "bootstrap.servers"                            "localhost:9092"
             StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   spaced.transit-serdes/transit-serdes
             StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG spaced.transit-serdes/transit-serdes
             "commit.interval.ms" 100}))

(defn streams
  [topology]
  (KafkaStreams. topology (stream-props)))

(defn store-put-item!
  [store item]
  (swap! (:records store) conj item))

(defn topology
  []
  (let [builder (StreamsBuilder.)
        logging (atom false)]

    ;; (.addStateStore builder
    ;;                 (Stores/keyValueStoreBuilder
    ;;                  (Stores/persistentKeyValueStore "test2")
    ;;                  (Serdes$StringSerde.)
    ;;                  (Serdes$LongSerde.)))
    
    (.addStateStore builder
                    (reify StoreBuilder
                      (build [_]
                        (state-store "Test"))
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
                        "Test")))
    
    (-> builder
        (.stream "objects")
        (.transform (reify TransformerSupplier
                      (get [this]
                        (let [context (atom nil)
                              store   (atom nil)]
                          (reify Transformer
                            (init [this ctx]
                              (reset! store (.getStateStore ctx "Test"))
                              (reset! context ctx))
                            (close [this])
                            (transform [this k v]
                              (store-put-item! @store [0 0])
                              (.forward @context "key" [0 0]))))))
                    (into-array String ["Test"]))

        (.groupByKey)
        (.reduce (reify Reducer
                   (apply [this agg val]
                     (str agg ":" val))))
        (.toStream)
        (.through "output-topic")
        )

    (.build builder)))

(with-open [driver (TopologyTestDriver. (topology) (stream-props))]
  (let [records (ConsumerRecordFactory. "investors"
                                        (spaced.transit-serdes/transit-serializer*)
                                        (spaced.transit-serdes/transit-serializer*))]
    (.. driver (pipeInput (.. records (create "objects" "key" {:apples "value"}))))
    (.. driver (pipeInput (.. records (create "objects" "key" {:apples "value"}))))
    (.. driver (pipeInput (.. records (create "objects" "key" {:apples "value"}))))
    (.. driver (pipeInput (.. records (create "objects" "key" {:apples "value"}))))
    (println :test @(:records (.getStateStore driver "Test")))
    (.. driver (readOutput "output-topic"
                           (spaced.transit-serdes/transit-deserializer*)
                           (spaced.transit-serdes/transit-deserializer*)))

    (.. driver (readOutput "output-topic"
                           (spaced.transit-serdes/transit-deserializer*)
                           (spaced.transit-serdes/transit-deserializer*)))    
    ))
