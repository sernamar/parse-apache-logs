(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]))

(def ^:private connection (atom nil))

(def ^:private db-params (-> "db-params.edn"
                             slurp
                             clojure.edn/read-string))

(defn get-connection []
  "Get a connection to the database."
  (reset! connection (-> db-params
                         jdbc/get-datasource
                         jdbc/get-connection)))

;;; ------------------- ;;;
;;; Database connection ;;;
;;; ------------------- ;;;

(defn- table-in-database? [table]
  "Check in a table exists in the database."
  (let [query (str "SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema='public' AND table_name='"
                   table
                   "')")]
    (:exists (jdbc/execute-one! @connection [query]))))

(defn create-log-table []
  "Create the 'log' table if it doesn't exist."
  (when-not (table-in-database? "log")
    (jdbc/execute-one! @connection ["CREATE TABLE log (id SERIAL PRIMARY KEY, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"])))

(defn close-connection []
  "Close the connection to the database."
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


