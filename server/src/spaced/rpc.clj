(ns spaced.rpc
  (:require [slacker.server.cluster :as server]
            [slacker.client.cluster :as client]
            [slacker.client :as cl]
            [spaced.api]))

(comment
  (def server
    (server/start-slacker-server [;;(the-ns 'spaced.api)
                                  {"slacker.example.api2" {"echo2" (fn [& args] {:server1 args})}}]
                                 2020
                                 :cluster {:name "test"
                                           :zk "localhost:2181"}
                                 :zk-session-timeout 5000
                                 :manager true))

  (def server2
    (server/start-slacker-server [;;(the-ns 'spaced.api)
                                  {"slacker.example.api2" {"echo2" (fn [& args] [:server2 args])}}]
                                 2021
                                 :cluster {:name "test"
                                           :zk "localhost:2181"}
                                 :zk-session-timeout 5000
                                 :manager true))

  (defn my-grouping
    [ns-name fn-name params slacker-client servers]
    (second servers))

  (let [sc (client/clustered-slackerc "test" "127.0.0.1:2181")]
    (with-open [_ @sc]
      (cl/call-remote sc "slacker.example.api2" "echo2" ["test" "one" "two"] :grouping :all :grouping-results :vector))))


