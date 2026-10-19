(ns jepsen.etcd.errors-test
  (:require [clojure.test :refer :all]
            [jepsen.etcd.client :as client])
  (:import (io.etcd.jetcd.common.exception EtcdExceptionFactory ErrorCode)))

(defn- server-stopped
  "The exception jetcd throws when the server stops during a call (#299)."
  []
  (EtcdExceptionFactory/newEtcdException ErrorCode/UNAVAILABLE
                                         "etcdserver: server stopped"))

(deftest server-stopped-is-known
  ; An idempotent watch fails and keeps its client and revision; a write
  ; may have taken effect.
  (is (= :fail (:type (client/with-errors {:f :watch} #{:watch}
                        (throw (server-stopped))))))
  (is (= :info (:type (client/with-errors {:f :write} #{}
                        (throw (server-stopped)))))))
