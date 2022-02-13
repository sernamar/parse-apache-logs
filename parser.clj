(ns parse-apache-logs.parser
  (:require [clojure.java.io :as io]))

(defn read-lines-lazily
  "Reads a file lazily, returning a lazy-sequence."
  [file]
  ;; See this stackoverflow answer https://stackoverflow.com/a/10462159 about why we cannot use 'with-open' if we want to later apply a function to each line read using 'line-seq'.
  ;; The code we'll use is the one copied from the following answer in the same question: https://stackoverflow.com/a/13312151
  (letfn [(helper [reader]
                  (lazy-seq
                    (if-let [line (.readLine reader)]
                      (cons line (helper reader))
                      (do (.close reader) nil))))]
    (helper (clojure.java.io/reader file))))

;; example
(let [file "data/log20170406/100k.log"]
  (count (read-lines-lazily file))) ;=> 100000
