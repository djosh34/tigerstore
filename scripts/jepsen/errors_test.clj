(ns jepsen.etcd.errors-test
  (:require [clojure.test :refer :all]
            [jepsen.etcd.client :as client])
  (:import (io.etcd.jetcd.common.exception EtcdExceptionFactory ErrorCode)))

(defn- etcd-exception
  "An exception jetcd throws when the server stops during a call, or the
  connection closes under it (#299)."
  [message]
  (EtcdExceptionFactory/newEtcdException ErrorCode/UNAVAILABLE message))

(deftest stopped-calls-are-known
  ; An idempotent watch fails and keeps its client and revision; a write
  ; may have taken effect.
  (doseq [message ["etcdserver: server stopped" "channel closed"]]
    (testing message
      (is (= :fail (:type (client/with-errors {:f :watch} #{:watch}
                            (throw (etcd-exception message))))))
      (is (= :info (:type (client/with-errors {:f :write} #{}
                            (throw (etcd-exception message)))))))))
