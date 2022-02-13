(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :refer [insert!]]))

(def database {:dbtype "sqlite"
               :dbname ":memory:"})

(def datasource (jdbc/get-datasource database))
(def connection (jdbc/get-connection datasource))

(defn test-database []
  (jdbc/execute-one! connection ["SELECT 1 + 2"]))

(defn create-table []
  (jdbc/execute-one! connection ["CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"]))

(defn insert-log []
  (insert! connection 'log {:ip "1.234.83.eib"
                              :date "2017-04-06"
                              :time "00:00:00"
                              :zone "0.0"
                              :cik "1319947.0"
                              :accession "0001225208-17-007392"
                              :doc "-index.htm"
                              :code "200.0"
                              :size "2770.0"
                              :idx "1.0"
                              :norefer "0.0"
                              :noagent "0.0"
                              :find "10.0"
                              :crawler "0.0"
                              :browser "fox"}))

(defn select-all []
  (jdbc/execute! connection ["SELECT * FROM log"]))

(defn drop-table []
  (jdbc/execute-one! connection ["DROP TABLE log"]))
