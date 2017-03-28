(ns snujure.lpass
  (:require [clojure.java.shell :as shell])
  (:gen-class))

;;; Constants
(def lpass-entry "opendata.socrata.com") ;Default entry for lastpass

;;; Invoke the lpass command line utility to get an entry associated with keyname
(defn lpass-get
  "Calls lastpass command line utility to fetch element(s) from
  lastpass entry. Takes a lastpass field command (e.g. --password,
  --username, --field=custom). Returns a string."
  [fieldcmd]
  (clojure.string/trim-newline (:out (shell/sh "lpass" "show" fieldcmd lpass-entry))))


;;; Get specific lpass fields
(def lpass-get-user (partial lpass-get "--username"))
(def lpass-get-pass (partial lpass-get "--password"))
(def lpass-get-token (partial lpass-get "--field=pubtoken"))
