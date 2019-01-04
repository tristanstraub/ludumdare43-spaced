(ns spaced.coordinator
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig TopologyTestDriver]
           [org.apache.kafka.streams.kstream Aggregator Initializer KeyValueMapper Materialized Transformer TransformerSupplier]
           [org.apache.kafka.streams.processor PunctuationType Punctuator]
           org.apache.kafka.streams.state.Stores
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(defn- to-props
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m]
      (.put ps  k v))
    ps))

(defn topology
  []
  (let [builder (StreamsBuilder.)]
    (.addStateStore builder
                    (Stores/keyValueStoreBuilder
                     (Stores/persistentKeyValueStore "partitions")
                     (spaced.transit-serdes/transit-serdes*)
                     (spaced.transit-serdes/transit-serdes*)))

    (-> builder
        (.stream "positions")
        (.transform (reify TransformerSupplier
                      (get [this]
                        (let [store (atom nil)
                              context (atom nil)]
                          (reify Transformer
                            (init [this ctx]
                              (reset! store (.getStateStore ctx "partitions"))
                              (reset! context ctx)

                              (.schedule ctx 1000
                                         PunctuationType/WALL_CLOCK_TIME
                                         (reify Punctuator
                                           (punctuate [_ _]
                                             (.forward ctx 0 (.get @store 0))))))
                            (close [this])
                            (transform [this _ pos]
                              (.put @store 0 (conj (or (.get @store 0) #{})
                                                   (.partition @context))))))))
                    (into-array String ["partitions"]))

        (.groupBy (reify KeyValueMapper
                    (apply [_ k v]
                      0)))
        (.aggregate (reify Initializer
                      (apply [_]
                        #{}))
                    (reify Aggregator
                      (apply [_ k v agg]
                        (apply conj agg v)))
                    (Materialized/as "all-partitions"))
        (.toStream)
        (.to "all-partitions"))

    ;; (-> builder
    ;;     (.stream "watermarks")
    ;;     (.groupBy (reify KeyValueMapper
    ;;                 (apply [_ k v]
    ;;                   0)))
    ;;     (.aggregate (reify Initializer
    ;;                   (apply [_]
    ;;                     #{}))
    ;;                 (reify Aggregator
    ;;                   (apply [_ k v agg]
    ;;                     (conj agg v)))
    ;;                 (Materialized/as "watermarks")))

    (.build builder)))

(defn test
  []
  (let [props   (to-props {"application.id"    "test23"
                           "bootstrap.servers" "localhost:9092"
                           StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   spaced.transit-serdes/transit-serdes
                           StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG spaced.transit-serdes/transit-serdes})
        factory (ConsumerRecordFactory. "positions"
                                        (spaced.transit-serdes/transit-serializer*)
                                        (spaced.transit-serdes/transit-serializer*))]

    (with-open [driver (TopologyTestDriver. (topology) props)]
      (.pipeInput driver (.create factory "positions" 1 1))
      (.pipeInput driver (.create factory "positions" 2 1))
      (.pipeInput driver (.create factory "positions" 3 1))
      (.pipeInput driver (.create factory "positions" 4 1))


      (let [{:strs [watermarks partitions]} (.getAllStateStores driver)]
        (doall (iterator-seq (.all watermarks)))
        (doall (iterator-seq (.all partitions)))
        (.advanceWallClockTime driver 100)

        (.deserialize (spaced.transit-serdes/transit-deserializer*)
                      nil nil (.value (.readOutput driver "all-partitions")))))))


(defn stream-props
  [app-id & [{:as options}]]
  (to-props (merge {"application.id"                               app-id
                    "bootstrap.servers"                            "localhost:9092"
                    StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   spaced.transit-serdes/transit-serdes
                    StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG spaced.transit-serdes/transit-serdes
                    "commit.interval.ms" 100
                    "num.stream.threads" (int 16)}
                   options)))

(defn streams
  [topology app-id & [options]]
  (KafkaStreams. topology (stream-props app-id options)))


(defonce ids (atom 0))
(defn id!
  []
  (swap! ids inc))

(defn app-id!
  []
  (str "test-" (id!)))

(defn produce!
  [p topic k v]
  (.send p (ProducerRecord. topic k v)))



(defn produce!
  [p topic k v]
  (.send p (ProducerRecord. topic k v)))

(defonce stream (atom nil))
(defonce stream2 (atom nil))

(comment (defonce p (KafkaProducer. (to-props {"bootstrap.servers" "localhost:9092"
                                               "key.serializer" spaced.transit-serdes/transit-serializer
                                               "value.serializer" spaced.transit-serdes/transit-serializer})))

         (dotimes [i (count (.partitionsFor p "positions"))]
           @(.send p (ProducerRecord. "positions" (int i) nil 0)))


         (let [t (topology)
               id (app-id!)]
           (.start (reset! stream (streams t id {"state.dir" "kafka-streams/partitions-6"}))))
         (.close @stream)

         (let [t (topology)
               id (app-id!)]
           (.start (reset! stream2 (streams t id {"state.dir" "kafka-streams/partitions-5"}))))
         (.close @stream2)

         @(produce! p "positions" 2 {:dummy (rand-int 1000)})

         (def c (KafkaConsumer. (to-props {"bootstrap.servers"  "localhost:9092"
                                           "group.id"           "test-1000"
                                           "key.deserializer"   spaced.transit-serdes/transit-deserializer
                                           "value.deserializer" spaced.transit-serdes/transit-deserializer})))

         (.subscribe c ["all-partitions"])

         (last (map #(.value %) (doall (iterator-seq (.iterator (.poll c 100))))))


         )
