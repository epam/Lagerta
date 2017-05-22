;;
;; Licensed to the Apache Software Foundation (ASF) under one or more
;; contributor license agreements.  See the NOTICE file distributed with
;; this work for additional information regarding copyright ownership.
;; The ASF licenses this file to You under the Apache License, Version 2.0
;; (the "License"); you may not use this file except in compliance with
;; the License.  You may obtain a copy of the License at
;;
;;      http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;

(ns jepsen.lagerta.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir "/opt/ignite")
(def binary  "bin/ignite.sh")
(def tmp-dir-base "Where should we put temporary files?" "/tmp/jepsen")
(def logfile (str dir "/ignite.log"))
(def pidfile (str dir "/ignite.pid"))

(defn install-ignite
  "Gets the given tarball URL, caching it in /tmp/jepsen/, and extracts its
  sole top-level directory to the given dest directory. Deletes
  current contents of dest. Returns dest."
  ([node url dest]
   (install-ignite node url dest false))
  ([node url dest force?]
   (let [local-file (nth (re-find #"file://(.+)" url) 1)
         file (or local-file
                  (do (c/exec :mkdir :-p tmp-dir-base)
                      (c/cd tmp-dir-base
                            (c/expand-path (cu/wget! url force?)))))
         tmpdir (cu/tmp-dir!)
         dest (c/expand-path dest)]
     ; Clean up old dest
     (c/exec :rm :-rf dest)
     (try
       (c/cd tmpdir
             ; Extract tarball to tmpdir
             (c/exec :unzip file "-d" tmpdir)

             ; Get tarball root paths
             (let [roots (cu/ls)]
               (assert (pos? (count roots)) "Tarball contained no files")
               (assert (= 1 (count roots))
                       (str "Tarball contained multiple top-level files: "
                            (pr-str roots)))

               ; Move root to dest
               (c/exec :mv (first roots) dest)))
       (catch RuntimeException e
         (condp re-find (.getMessage e)
           #"tar: Unexpected EOF"
           (if local-file
             ; Nothing we can do to recover here
             (throw (RuntimeException.
                      (str "Local tarball " local-file " on node " (name node)
                           " is corrupt: unexpected EOF.")))
             (do (info "Retrying corrupt tarball download")
                 (c/exec :rm :-rf file)
                 (install-ignite node url dest)))

           ; Throw by default
           (throw e)))
       (finally
         ; Clean up tmpdir
         (c/exec :rm :-rf tmpdir))))
   dest))

(defn ignite-control
  "Ignite"
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "installing ignite")
      (c/su
        (let [url (str "https://archive.apache.org/dist/ignite/1.9.0/apache-ignite-hadoop-1.9.0-bin.zip")]
          (install-ignite c/*host* url dir)))
      (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir   dir}
        binary)

      (Thread/sleep 10000))

    (teardown! [_ test node]
      (info node "tearing down ignite")
      (cu/stop-daemon! binary pidfile)
      (c/su
        (c/exec :rm :-rf dir)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn lagerta-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "lagerta"
          :os   debian/os
          :db   (ignite-control)}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn lagerta-test})
                   (cli/serve-cmd))
            args))