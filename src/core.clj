(ns parse-apache-logs.core
  (:require [parse-apache-logs.database :refer [get-connection
                                                create-log-table
                                                insert-logs
                                                select-all
                                                close-connection]]
            [parse-apache-logs.parser :refer [parse-apache-file]]))

(defn parse-and-insert [file]
  "Parse an Apache log file and insert the data in the 'log' table of a database."
  (-> file
      parse-apache-file
      insert-logs))


;; example
(get-connection)
(create-log-table)
(let [file "data/log20170406/10.log"]
  (parse-and-insert file))
(select-all)
(close-connection)
