(ns jepsen.etcd.diag299-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.etcd.watch :as watch])
  (:import (io.etcd.jetcd.common.exception EtcdExceptionFactory ErrorCode)))

(deftest original-history-replay
  (let [history (with-open [r (io/reader (System/getenv "DIAG299_HISTORY"))]
                  (mapv edn/read-string (line-seq r)))
        test {:concurrency 6}
        checks (watch/checker)
        logs (watch/per-thread-logs test history)
        process-log (fn [p]
                      (->> history
                           (filter #(and (= p (:process %)) (= :ok (:type %))
                                         (#{:watch :final-watch} (:f %))))
                           (mapcat (comp :log :value)) vec))
        p4 (process-log 4)
        p3 (process-log 3)
        p5 (process-log 5)
        p9 (process-log 9)
        p11 (process-log 11)
        ;; Diagnostic only: keep each thread's latest incarnation. The proposed
        ;; fix does NOT change the checker or discard any history.
        latest (filterv #(not (#{3 5} (:process %))) history)]
    (println "Original verdict" (select-keys (checker/check checks test history {})
                                            [:valid? :revisions :deltas]))
    (println "Process lengths" (mapv #(vector % (count (process-log %))) [3 4 5 9 11]))
    (is (false? (:valid? (checker/check checks test history {}))))
    (is (= 1391 (count p4)))
    (is (= p4 p9 p11))
    (is (= p3 (subvec p9 0 898)))
    (is (= p5 (subvec p11 0 1036)))
    (is (= (vec (get logs 3)) (into p3 p9)))
    (is (= (vec (get logs 5)) (into p5 p11)))
    (is (true? (:valid? (checker/check checks test latest {}))))))

(deftest stopped-watch-retains-resume-revision
  (let [revision (atom 899)
        c (watch/->Client nil "w" (atom 899) revision nil)
        op {:process 3 :type :invoke :f :watch :value nil}
        failure (EtcdExceptionFactory/newEtcdException
                  ErrorCode/UNAVAILABLE "etcdserver: server stopped")]
    ;; This is the exact Jetcd exception class and text from the red artifact.
    ;; An uncaught exception makes Jepsen emit :info and replace the client.
    (with-redefs [watch/watch-for (fn [& _] (throw failure))]
      (let [res (client/invoke! c {} op)]
        (is (= :fail (:type res)))
        (is (= [:server-stopped "etcdserver: server stopped"] (:error res)))))
    (is (= 899 @revision))
    (with-redefs [watch/watch-for
                  (fn [_ process key rev _]
                    (is (= [3 "w" 899] [process key rev]))
                    {:revision 900 :events [] :log [909]})]
      (is (= :ok (:type (client/invoke! c {} op)))))
    (is (= 900 @revision))))

(deftest checker-still-rejects-a-missing-event
  (let [history (mapv (fn [process log]
                        {:process process :type :ok :f :watch
                         :value {:revision 4 :log log}})
                      [3 4 5] [[0 1 2] [0 2] [0 1 2]])]
    (is (false? (:valid? (checker/check (watch/checker)
                                        {:concurrency 6} history {}))))))
