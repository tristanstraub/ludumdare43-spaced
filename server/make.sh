#!/bin/bash
mkdir -p target/classes
clojure -C:aot -A:pack
