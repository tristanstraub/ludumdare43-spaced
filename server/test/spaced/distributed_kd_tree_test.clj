(ns spaced.distributed-kd-tree-test
  (:require [spaced.distributed-kd-tree :as distributed-kd-tree]
            spaced.transit-serdes
            [kdtree :as kdtree])
  (:import java.util.Properties
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(defn- to-props
  [m]
  (let [ps (Properties.)]
    (doseq [[k v] m]
      (.put ps  k v))
    ps))

(let [objects [{:object/id 1 :object/position [0 0]}
               {:object/id 2 :object/position [1000 1000]}]
      props   (to-props {"application.id"    "test23"
                         "bootstrap.servers" "localhost:9092"
                         StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   spaced.transit-serdes/transit-serdes
                         StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG spaced.transit-serdes/transit-serdes})
      factory (ConsumerRecordFactory. "objects"
                                      (spaced.transit-serdes/transit-serializer*)
                                      (spaced.transit-serdes/transit-serializer*))]
  (with-open [driver (TopologyTestDriver. (distributed-kd-tree/kdtree-topology) props)]
    (doseq [object objects]
      (.pipeInput driver (.create factory "objects" (distributed-kd-tree/position-to-key (:object/position object)) object)))

    (let [{:strs [kdtree]} (.getAllStateStores driver)]
      (:object (meta (kdtree/nearest-neighbor @(:tree kdtree) [600 600]))))))




