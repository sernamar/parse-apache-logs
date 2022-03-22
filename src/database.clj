(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]))

(def connection (atom nil))

(def db-params (-> "db-params.edn"
                   slurp
                   clojure.edn/read-string))

(defn- get-database-connection [db-params]
  "Get a connection to the database using the provided connection parameters."
  (reset! connection (-> db-params
                           jdbc/get-datasource
                           jdbc/get-connection)))

;;; ------------------- ;;;
;;; Database connection ;;;
;;; ------------------- ;;;

(defn- create-log-table []
  "Create a table called 'log' into the database."
  (jdbc/execute-one! @connection ["CREATE TABLE log (id SERIAL PRIMARY KEY, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"]))

(defn- table-in-database? [table]
  "Check in a table exists in the database."
  (let [query (str "SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema='public' AND table_name='"
                   table
                   "')")]
    (:exists (jdbc/execute-one! @connection [query]))))

(defn connect-to-database []
  "Connect to the database, creating a table called 'log' if it doesn't exist."
  (get-database-connection db-params)
  (when-not (table-in-database? "log")
    (create-log-table)))

(defn disconnect-from-database []
  "Disconnect from the database."
  (.close @connection))

;;; ---------------- ;;;
;;; Database queries ;;;
;;; ---------------- ;;;

(defn- insert-log [log]
  "Insert a string with the content of a log line into the 'log' table."
  (jdbc.sql/insert! @connection 'log log))

(defn insert-logs [logs]
  "Insert a sequence of logs into the 'log' table."
  (map insert-log logs))

(defn select-all []
  "Select all from the 'log' table."
  (jdbc/execute! @connection ["SELECT * FROM log"]))


