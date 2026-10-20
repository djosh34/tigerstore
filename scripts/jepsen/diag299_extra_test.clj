(ns jepsen.etcd.diag299-extra-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.etcd.client :as c]
            [jepsen.etcd.watch :as watch])
  (:import (io.etcd.jetcd Watch$Watcher)
           (io.etcd.jetcd.common.exception EtcdExceptionFactory ErrorCode)))

(defn process-ops [history p]
  (filter #(and (= p (:process %)) (= :ok (:type %))
                (#{:watch :final-watch} (:f %))) history))

(defn process-log [history p]
  (vec (mapcat (comp :log :value) (process-ops history p))))

(deftest second-artifact-replay
  (let [history (with-open [r (io/reader (System/getenv "DIAG299_HISTORY"))]
                  (mapv edn/read-string (line-seq r)))
        test {:concurrency 6}
        check #(checker/check (watch/checker) test % {})
        logs (watch/per-thread-logs test history)
        p3 (process-log history 3)
        p4 (process-log history 4)
        p5 (process-log history 5)
        p10 (process-log history 10)
        crash (nth history 352)
        stopped (nth history 2856)
        ;; Evidence counterfactual only. The proposed fix never drops history.
        latest (filterv #(not= 4 (:process %)) history)]
    (println "ARTIFACT process lengths"
             (mapv #(vector % (count (process-log history %))) [3 4 5 10]))
    (println "ARTIFACT crash" (select-keys crash [:index :process :type :f])
             (first (get-in crash [:exception :via])))
    (println "ARTIFACT stopped" stopped)
    (println "ARTIFACT thread lengths" (into {} (map (fn [[k v]] [k (count v)]) logs)))
    (is (= :info (:type crash)))
    (is (= "channel closed" (get-in crash [:exception :via 0 :message])))
    (is (= 147 (count p4)))
    (is (= 1265 (count p3)))
    (is (= p3 p5 p10))
    (is (= p4 (subvec p10 0 147)))
    (is (= (vec (get logs 4)) (into p4 p10)))
    (is (false? (:valid? (check history))))
    (is (= {3 1266 4 1266 5 1266} (:revisions (check history))))
    (is (= [{:thread 4 :edit-distance 147}]
           (mapv #(select-keys % [:thread :edit-distance]) (:deltas (check history)))))
    (is (true? (:valid? (check latest))))
    ;; Every incarnation is strictly monotone, including all recorded event
    ;; revisions. The duplicate is between incarnations, not within a stream.
    (doseq [p [3 4 5 10]]
      (is (= (count (process-log history p))
             (count (distinct (process-log history p)))))
      (let [revs (for [op (process-ops history p)
                       response (get-in op [:value :events])
                       event (:events response)]
                   (get-in event [:kv 1 :mod-revision]))]
        (is (apply < revs))))
    (is (= :fail (:type stopped)))
    (is (= [:server-stopped "etcdserver: server stopped"] (:error stopped)))
    (is (= [3] (vec (distinct (map :process (filter #(and (number? (:process %))
                                                         (= 3 (mod (:process %) 6))) history))))))
    (let [before (last (filter #(< (:index %) 2856) (process-ops history 3)))
          after (first (filter #(> (:index %) 2856) (process-ops history 3)))]
      (is (= 1145 (get-in before [:value :revision])))
      (is (= 1146 (-> after :value :events first :events first :kv second :mod-revision)))
      (println "ARTIFACT server-stopped resumes after" (get-in before [:value :revision])
               "at" (-> after :value :events first :events first :kv second :mod-revision)))))

(defn event [revision value]
  {:events [{:type :put :kv ["w" {:mod-revision revision :value value}]}]})

(defn interrupted-watch [message error-type]
  (let [revision (atom 148)
        max-revision (atom 148)
        cli (watch/->Client nil "w" max-revision revision nil)
        op {:process 4 :type :invoke :f :watch :value nil}
        failure (EtcdExceptionFactory/newEtcdException ErrorCode/UNAVAILABLE message)
        starts (atom [])
        fail? (atom true)
        partial (atom nil)]
    ;; Exercise the real watch state, callback error/completion path, error
    ;; mapping and invoke!, not a hand-written substitute for their behavior.
    (with-redefs [c/watch
                  (fn [_ key start update error complete]
                    (swap! starts conj start)
                    (update (event 149 150))
                    (update (event 150 152))
                    (when @fail? (error failure))
                    (reify Watch$Watcher
                      (close [_] (complete))
                      (requestProgress [_])))
                  watch/watch-for
                  (fn [conn process key rev _]
                    @(watch/watch conn process key rev))]
      (reset! partial (client/invoke! cli {} op))
      (is (= :fail (:type @partial)))
      (is (= [error-type message] (:error @partial)))
      (is (nil? (:value @partial)))
      (is (= 148 @revision))
      (is (= 148 @max-revision))
      (reset! fail? false)
      (let [res (client/invoke! cli {} op)]
        (is (= :ok (:type res)))
        (is (= [150 152] (get-in res [:value :log])))
        (is (= 150 @revision))
        (is (= [149 149] @starts))
        (println "CALLBACK REPLAY" message "starts" @starts
                 "failed value" (:value @partial) "resumed log" (get-in res [:value :log]))))))

(deftest channel-closed-watch-retains-revision
  (interrupted-watch "channel closed" :channel-closed))

(deftest server-stopped-watch-retains-revision
  (interrupted-watch "etcdserver: server stopped" :server-stopped))

(deftest channel-closed-write-is-indeterminate
  (let [failure (EtcdExceptionFactory/newEtcdException ErrorCode/UNAVAILABLE "channel closed")]
    (is (= :info (:type (c/with-errors {:f :write} #{} (throw failure)))))))

(deftest checker-still-rejects-real-duplicates-and-loss
  (doseq [log [[0 1 1 2] [0 2]]]
    (let [history (mapv (fn [p values]
                          {:process p :type :ok :f :watch
                           :value {:revision 4 :log values}})
                        [3 4 5] [[0 1 2] log [0 1 2]])]
      (is (false? (:valid? (checker/check (watch/checker) {:concurrency 6} history {})))))))
