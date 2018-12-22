(ns spaced.topology
  (:require [spaced.transit-serdes])
  (:import java.util.Properties
           [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization Serdes Serdes$LongSerde Serdes$StringSerde StringDeserializer StringSerializer]
           [org.apache.kafka.streams KeyValue KafkaStreams StreamsBuilder StreamsConfig TopologyDescription$Processor TopologyDescription$Sink TopologyDescription$Source TopologyTestDriver]
           [org.apache.kafka.streams.kstream Transformer Reducer TransformerSupplier ValueTransformerSupplier]
           org.apache.kafka.streams.state.Stores
           org.apache.kafka.streams.test.ConsumerRecordFactory
           (org.apache.kafka.streams.processor ProcessorSupplier Processor)))

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

(defn topology
  []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "objects")
        (.join (reify AggregatorSupplier
                      (get [this]
                        (reify Processor
                          (init [this ctx])
                          (close [this])
                          (process [this k v]
                            nil)))))
        (.reduce (reify Reducer
                   (apply [this agg val]
                     (str agg ":" val))))
        (.toStream)
        (.through "output-topic"))
    (.build builder)))

(with-open [driver (TopologyTestDriver. (topology) (stream-props))]
  (let [records (ConsumerRecordFactory. "investors"
                                        (spaced.transit-serdes/transit-serializer*)
                                        (spaced.transit-serdes/transit-serializer*))]
    (.. driver (pipeInput (.. records (create "objects" "key" {:apples "value"}))))
    (.. driver (readOutput "output-topic"
                           (spaced.transit-serdes/transit-deserializer*)
                           (spaced.transit-serdes/transit-deserializer*)))))
