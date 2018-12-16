(ns spaced.distance
  (:require [kdtree :as kdtree]))

(def points [[8 8] [3 1] [6 6] (with-meta [7 7] {:object 'x}) [1 3] [4 4] [5 5]])
;; Build a kdtree from a set of points
(def tree (kdtree/build-tree points))
(println "Tree:" tree)

(meta (second (kdtree/nearest-neighbor tree [8 8] 2)))
