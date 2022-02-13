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

(defn parse-file
  "Parse the file lazily using the parser function."
  [file parser]
  (parser (read-lines-lazily file)))

(def apache-log-pattern
  (let [ip-regex #"(\d{1,3}\.\d{1,3}\.\d{1,3}.\w{1,3})"
        date-regex #"(\d{4}-\d{2}-\d{2})"
        time-regex #"(\d{2}:\d{2}:\d{2})"
        zone-regex #"(\d\.0)"
        cik-regex #"(\d{1,10}\.0)"
        accession-regex #"(\d+-\d{2}\-\d+)"
        doc-regex #"(.+)"
        code-regex #"(\d{3}\.0)"
        size-regex #"(\d+.0)"
        idx-regex #"([0,1].0)"
        norefer-regex #"([0,1].0)"
        noagent-regex #"([0,1].0)"
        find-regex #"(\d{1,10}\.0)"
        crawler-regex #"([0,1].0)"
        browser-regex #"(\w{0,3})"]
    (re-pattern 
     (clojure.string/join "," (list ip-regex date-regex time-regex zone-regex cik-regex
                                    accession-regex doc-regex code-regex size-regex
                                    idx-regex norefer-regex noagent-regex find-regex
                                    crawler-regex browser-regex)))))

(defn apache-parser
  "
  Parses an Apache log line that has the following format:
  ip,date,time,zone,cik,accession,doc,code,size,idx,norefer,noagent,find,crawler,browser

  Returns a list of strings with the parsed values.
  "
  [log-line]
  (let [[_ ip date time zone cik accession doc code size idx norefer noagent find crawler browser] (re-matches apache-log-pattern log-line)]
    (list ip date time zone cik
          accession doc code size idx
          norefer noagent find crawler browser)))

;; example
(let [log-line "1.234.83.eib,2017-04-06,00:00:00,0.0,1319947.0,0001225208-17-007392,-index.htm,200.0,2770.0,1.0,0.0,0.0,10.0,0.0,fox"]
  (apache-parser log-line))
;; ("1.234.83.eib"
;;  "2017-04-06"
;;  "00:00:00"
;;  "0.0"
;;  "1319947.0"
;;  "0001225208-17-007392"
;;  "-index.htm"
;;  "200.0"
;;  "2770.0"
;;  "1.0"
;;  "0.0"
;;  "0.0"
;;  "10.0"
;;  "0.0"
;;  "fox")
