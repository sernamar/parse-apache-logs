(ns parse-apache-logs.core
  (:require [parse-apache-logs.database :refer [initialize-database
                                                insert-logs
                                                select-all
                                                close-database]]
            [parse-apache-logs.parser :refer [parse-apache-file]]))

(defn parse-and-insert [file]
  (-> file
      parse-apache-file
      insert-logs))


;; example
(initialize-database)
(let [file "data/log20170406/10.log"]
  (parse-and-insert file))
(select-all)
(close-database)
