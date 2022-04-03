(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]))

(def ^:private db-params (-> "config.edn"
                             slurp
                             clojure.edn/read-string
                             :db-params))

;;; ------------------- ;;;
;;; Database connection ;;;
;;; ------------------- ;;;

(defn get-connection
  "Get a connection to the database."
  []
  (-> db-params
       jdbc/get-datasource
       jdbc/get-connection))

(defn- table-exists?
  "Check if a table exists in the database."
  [db table-name]
  (-> (jdbc/execute-one! db
                         ["SELECT EXISTS (
                             SELECT NULL
                             FROM INFORMATION_SCHEMA.tables
                             WHERE table_schema = 'public'
                               AND table_name = ?
                           ) AS result"
                          table-name])
      :result))

(defn create-log-table
  "Create the 'log' table if it doesn't exist."
  [db]
  (when-not (table-in-database? "log")
    (jdbc/execute-one! db
                       ["CREATE TABLE log (id SERIAL PRIMARY KEY, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"])))

(defn close-connection
  "Close the connection to the database."
  [db]
  (.close db))

;;; ---------------- ;;;
;;; Database queries ;;;
;;; ---------------- ;;;

(defn- insert-log
  "Insert the content of a log line into the 'log' table."
  [db log]
  (jdbc.sql/insert! db 'log log))

(defn insert-logs 
  "Insert a sequence of logs into the 'log' table."
  [db logs]
  (map #(insert-log db %1) logs))

(defn select-all
  "Select all from the 'log' table."
  [db]
  (jdbc/execute! db ["SELECT * FROM log"]))


