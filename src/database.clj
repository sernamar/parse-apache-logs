(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]))

(def connection (atom nil))

(defn- create-database []
  (let [database {:dbtype "sqlite" :dbname ":memory:"}]
    (reset! connection (-> database
                           jdbc/get-datasource
                           jdbc/get-connection))))

(defn- create-log-table []
  (jdbc/execute-one! @connection ["CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"]))

(defn- insert-log [log]
  (jdbc.sql/insert! @connection 'log log))

(defn insert-logs [logs]
  (map insert-log logs))

(defn- select-all []
  (jdbc/execute! @connection ["SELECT * FROM log"]))

(defn- close-connection []
  (.close @connection))

(defn initialize-database []
  (create-database)
  (create-log-table))

(defn close-database []
  (close-connection))
