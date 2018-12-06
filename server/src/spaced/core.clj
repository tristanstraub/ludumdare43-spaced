(ns spaced.core
  (:import java.util.Properties
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization Serdes StringDeserializer StringSerializer]
           [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig TopologyDescription$Processor TopologyDescription$Sink TopologyDescription$Source TopologyTestDriver]
           org.apache.kafka.streams.kstream.Reducer
           org.apache.kafka.streams.test.ConsumerRecordFactory))


(defn setup!
  []
  (let [props (Properties.)
        numPartitions 3
        replicationFactor 1]
    (.. props (setProperty AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"))
    (let [client (.. AdminClient (create props))]
      (.. client (createTopics [(NewTopic. "new-topic-name"
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
                                     "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                     "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}))]
    (.send p (ProducerRecord. "input-topic" "key" "value"))
    (.close p)))

(defn consume!
  []
  (let [c (KafkaConsumer. (to-props {"bootstrap.servers" "localhost:9092"
                                     "group.id" "test"
                                     "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                     "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"}))]
    (.. c (subscribe ["input-topic"]))
    (let [result (.poll c 100)]
      (.close c)
      (map (juxt #(.key %) #(.value %))
           (iterator-seq (.iterator result))))))

(defn stream!
  []
  (let [builder   (StreamsBuilder.)
        investor  (.stream builder "investors")
        portfolio (.globalTable builder "porfolios")
        ;;        #_[institutions retail]
        T         (.. investor
                      ;; (join portfolio
                      ;;       (reify KeyValueMapper
                      ;;         (apply [this k v]
                      ;;           v))
                      ;;       (reify ValueJoiner
                      ;;         (apply [this a b]
                      ;;           a)))
                      (to "output-topic"))
        topology  (.build builder)]

    (let [props       (to-props {"application.id"    "test17"
                                 "bootstrap.servers" "localhost:9092"})
          test-driver (TopologyTestDriver. topology props)
          factory     (ConsumerRecordFactory. "investors"
                                              (StringSerializer.)
                                              (StringSerializer.))]
      (.. test-driver (pipeInput (.. factory (create "investors" "key" "value"))))
      (.. test-driver (pipeInput (.. factory (create "investors" "key" "value2"))))
      (let [result [(.. test-driver (readOutput "output-topic"
                                                (StringDeserializer.)
                                                (StringDeserializer.)))
                    (.. test-driver (readOutput "output-topic"
                                                (StringDeserializer.)
                                                (StringDeserializer.)))]]
        (.close test-driver)
        result))))

(def p (KafkaProducer. (to-props {"bootstrap.servers" "localhost:9092"
                                  "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                                  "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})))

(def c (KafkaConsumer. (to-props {"group.id" "consumer"
                                  "bootstrap.servers" "localhost:9092"
                                  "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                                  "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})))

(defn identify-node
  [node]
  (cond (instance? TopologyDescription$Sink node)
        (format "sink: %s" (.topic node))
        (instance? TopologyDescription$Source node)
        (format "source: %s" (.topics node))
        (instance? TopologyDescription$Processor node)
        (format "processor: %s" (.stores node))
        :else nil
        ))

(let [builder (StreamsBuilder.)]
  (-> builder
      (.stream "test")
      (.groupByKey)
      (.reduce (reify Reducer
                 (apply [this agg val]
                   (str agg ":" val))))
      (.toStream)
      (.to "output-topic")
      )
  (->> builder
      (.build)
      (.describe)
      (.subtopologies)
      (mapcat #(.nodes %))
      (map identify-node)

      ))

(let [builder   (StreamsBuilder.)]

  (-> builder
      (.stream "test")
      (.groupByKey)
      (.reduce (reify Reducer
                 (apply [this agg val]
                   (str agg ":" val))))
      (.toStream)
      (.through "output-topic")
      )

    (let [streams (KafkaStreams. (.build builder)
                               (to-props {"application.id"                               "test21"
                                          "bootstrap.servers"                            "localhost:9092"
                                          StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   (.getClass (Serdes/String))
                                          StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/String))
                                          "commit.interval.ms" 100})
                               )]
      (def streams streams))

  #_(let [topology    (.build builder)
        props       (to-props {"application.id"                               "test20"
                               "bootstrap.servers"                            "localhost:9092"
                               StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   (.getClass (Serdes/String))
                               StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getClass (Serdes/String))})
        test-driver (TopologyTestDriver. topology props)
        factory     (ConsumerRecordFactory. "investors"
                                            (StringSerializer.)
                                            (StringSerializer.))]

    (.. test-driver (pipeInput (.. factory (create "test" "key" "value"))))
    (.. test-driver (pipeInput (.. factory (create "test" "key" "value2"))))
    (.. test-driver (pipeInput (.. factory (create "test" "key" "value3"))))
    (.. test-driver (pipeInput (.. factory (create "test" "key" "value4"))))

    (let [result [(.. test-driver (readOutput "output-topic"
                                              (StringDeserializer.)
                                              (StringDeserializer.)))
                  (.. test-driver (readOutput "output-topic"
                                              (StringDeserializer.)
                                              (StringDeserializer.)))
                  (.. test-driver (readOutput "output-topic"
                                              (StringDeserializer.)
                                              (StringDeserializer.)))
                  (.. test-driver (readOutput "output-topic"
                                              (StringDeserializer.)
                                              (StringDeserializer.)))]]
      (.close test-driver)
      result)))
