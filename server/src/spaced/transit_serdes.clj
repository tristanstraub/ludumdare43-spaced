(ns spaced.transit-serdes
  (:require [cognitect.transit :as transit])
  (:import (org.apache.kafka.common.serialization Serializer Deserializer)
           (java.io ByteArrayOutputStream ByteArrayInputStream)
           (org.apache.kafka.common.serialization Serde)))

(deftype TransitSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
    (when data
      (with-open [output (ByteArrayOutputStream.)]
        (-> (transit/writer output :json)
            (transit/write data))
        (.toByteArray output))))
  (close [_]))

(deftype TransitDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
    (when data
      (-> (transit/reader (ByteArrayInputStream. data) :json)
          (transit/read))))
  (close [_]))

(deftype TransitSerdes []
  Serde
  (serializer [_]
    (TransitSerializer.))

  (deserializer [_]
    (TransitDeserializer.))

  (configure [_ _ _])

  (close [_]))

(def transit-serializer TransitSerializer)
(def transit-deserializer TransitDeserializer)
(def transit-serdes TransitSerdes)

(defn transit-serdes*
  []
  (TransitSerdes.))

(defn transit-serializer*
  []
  (TransitSerializer.))

(defn transit-deserializer*
  []
  (TransitDeserializer.))
