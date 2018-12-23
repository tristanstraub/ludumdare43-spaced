(ns spaced.distributed-kd-tree
  (:require spaced.transit-serdes
            [kdtree :as kdtree])
  (:import java.util.Properties
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig TopologyTestDriver]
           [org.apache.kafka.streams.kstream Reducer Transformer TransformerSupplier]
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
  (swap! (:records store) kdtree/insert item))

(defn topology
  []
  (let [builder (StreamsBuilder.)
        logging (atom false)]

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
                              (println :v v)
                              (swap! (:records @store) kdtree/insert v)
                              (.forward @context "key" (vec (kdtree/nearest-neighbor @(:records @store)
                                                                                     v
                                                                                     2))))))))
                    (into-array String ["Test"]))

        ;; (.groupByKey)
        ;; (.reduce (reify Reducer
        ;;            (apply [this agg val]
        ;;              (conj agg val))))
        ;;        (.toStream)
        (.through "output-topic"))

    (.build builder)))

(with-open [driver (TopologyTestDriver. (topology) (stream-props))]
  (let [records (ConsumerRecordFactory. "investors"
                                        (spaced.transit-serdes/transit-serializer*)
                                        (spaced.transit-serdes/transit-serializer*))]
    (.. driver (pipeInput (.. records (create "objects" "key" [0 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [1 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [10 0]))))
    (.. driver (pipeInput (.. records (create "objects" "key" [9.5 0]))))

    (dotimes [i 10]
      (println (.. driver (readOutput "output-topic"
                                      (spaced.transit-serdes/transit-deserializer*)
                                      (spaced.transit-serdes/transit-deserializer*)))))))
