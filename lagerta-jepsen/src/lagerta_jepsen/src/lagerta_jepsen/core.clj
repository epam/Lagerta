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

(ns lagerta_jepsen.core
	(:gen-class)
	(:require [clojure.tools.logging :refer :all]
              [clojure.string :as str]
              [jepsen [cli :as cli]
                      [control :as c]
                      [db :as db]
                      [tests :as tests]]
              [jepsen.control.util :as cu]
              [jepsen.os.debian :as debian]))


(def dir     "/opt/etcd") 



(defn etcd-control
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing etcd" version)
	  (c/su
       (let [url (str "https://storage.googleapis.com/etcd/" version
                  "/etcd-" version "-linux-amd64.tar.gz")]
        (cu/install-tarball! c/*host* url dir))))

    (teardown! [_ test node]
      (info node "tearing down etcd"))))

	  
(defn etcd-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh, :concurrency, ...), constructs a test map."
  [opts]  
  (merge tests/noop-test
		 {:name "etcd"
          :os debian/os
          :db (etcd-control "v3.1.5")}
         opts))			

	  
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn etcd-test})
                   (cli/serve-cmd))
            args))