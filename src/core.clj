(ns parse-apache-logs.core
  (:require [parse-apache-logs.database :refer [get-connection
                                                create-log-table
                                                insert-logs
                                                select-all
                                                close-connection]]
            [parse-apache-logs.parser :refer [parse-apache-file]]))

(defn parse-and-insert
  "Parse an Apache log file and insert the data in the 'log' table of a database."
  [db file]
  (insert-logs db (parse-apache-file file)))


;; example
(def db (get-connection))
(create-log-table db)
(let [file "data/log20170406/10.log"]
  (parse-and-insert db file))
(select-all db)
(close-connection db)
