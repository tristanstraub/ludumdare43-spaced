(ns spaced.main
  (:gen-class))

(defn -main
  [& argv]
  (require 'spaced.server)
  (apply (Clojure/var "spaced.server" "-main") argv))
