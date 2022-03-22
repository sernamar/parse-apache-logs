(ns parse-apache-logs.database
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as jdbc.sql]))

(def connection (atom nil))

(def db-params (-> "db-params.edn"
                   slurp
                   clojure.edn/read-string))

(defn- get-database-connection []
  (let [database db-params]
    (reset! connection (-> database
                           jdbc/get-datasource
                           jdbc/get-connection))))

(defn- create-log-table []
  (jdbc/execute-one! @connection ["CREATE TABLE log (id SERIAL PRIMARY KEY, ip TEXT, date TEXT, time TEXT, zone TEXT, cik TEXT, accession TEXT, doc TEXT, code TEXT, size TEXT, idx TEXT, norefer TEXT, noagent TEXT, find TEXT, crawler TEXT, browser TEXT)"]))

(defn- table-in-database? [table]
  (let [query (str "SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema='public' AND table_name='"
                   table
                   "')")]
    (:exists (jdbc/execute-one! @connection [query]))))

(defn- insert-log [log]
  (jdbc.sql/insert! @connection 'log log))

(defn insert-logs [logs]
  (map insert-log logs))

(defn select-all []
  (jdbc/execute! @connection ["SELECT * FROM log"]))

(defn- close-connection []
  (.close @connection))

(defn open-database []
  (get-database-connection)
  (when-not (table-in-database? "log")
    (create-log-table)))

(defn close-database []
  (close-connection))
