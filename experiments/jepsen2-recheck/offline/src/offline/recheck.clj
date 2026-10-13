(ns offline.recheck
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [knossos.wgl :as wgl]
            [knossos.linear :as linear]
            [knossos.competition :as competition]
            [offline.versioned-register :as register])
  (:import (java.io PushbackReader)))

(defn read-history [path]
  (with-open [r (PushbackReader. (io/reader path))]
    (loop [ops []]
      (let [op (edn/read {:eof ::eof} r)]
        (if (= ::eof op) ops
          (do (assert (map? op) "History must consist of EDN operation maps")
              (recur (conj ops op))))))))

(defn clients [history]
  (doseq [op history]
    (assert (or (integer? (:process op)) (= :nemesis (:process op)))
            (str "Unexpected process: " (pr-str op))))
  (filterv #(integer? (:process %)) history))

(defn keyed-value? [v]
  (and (vector? v) (= 2 (count v)) (integer? (first v))
       (vector? (second v)) (= 2 (count (second v)))))

(defn validate-full [history]
  (let [ops (clients history)]
    (assert (seq ops) "No client operations")
    (doseq [op ops]
      (assert (#{:invoke :ok :fail :info} (:type op)) "Unexpected operation type")
      (assert (#{:read :write :cas} (:f op)) "Unexpected client function")
      (assert (keyed-value? (:value op)) "Expected [key [version value]]"))
    ;; Verify invocation/completion identity. Never match across keys.
    (reduce (fn [pending op]
              (let [p (:process op)]
                (if (= :invoke (:type op))
                  (do (assert (not (contains? pending p)) "Overlapping calls on a process")
                      (assoc pending p op))
                  (let [call (get pending p)]
                    (assert call "Completion without invocation")
                    (assert (= (:f call) (:f op)) "Function changed at completion")
                    (assert (= (first (:value call)) (first (:value op)))
                            "Key changed at completion")
                    (dissoc pending p))))) {} ops)
    (sort (distinct (map #(first (:value %)) ops)))))

(defn projection [history k]
  ;; This matches independent/subhistories on serialized tuple data.
  ;; Nonclient nemesis records remain, and comparison checks those too.
  (into [] (keep (fn [op]
                  (if (integer? (:process op))
                    (when (= k (first (:value op)))
                      (assoc op :value (second (:value op))))
                    op))) history))

(defn check-one [full-path saved-path key algorithm out-path]
  (let [full (read-history full-path)
        keys (validate-full full)
        k (parse-long key)
        _ (assert (some #{k} keys) "Key absent from full history")
        projected (projection full k)
        saved (read-history saved-path)
        _ (assert (= projected saved)
                  "Saved independent history differs from full-history projection")
        ops (clients projected)
        analyze (case algorithm "wgl" wgl/analysis
                                "linear" linear/analysis
                                "competition" competition/analysis)
        ;; No time-limit option: the shell kills the whole JVM on timeout,
        ;; so an aborted worker cannot overlap a later key or solver.
        result (analyze (register/->VersionedRegister 0 nil) ops)
        verdict (:valid? result)
        summary {:valid? verdict
                 :algorithm algorithm
                 :key k
                 :full-history full-path
                 :saved-independent-history saved-path
                 :projection-identical? true
                 :client-event-count (count ops)
                 :client-info-count (count (filter #(= :info (:type %)) ops))
                 :unknown-mutation-count
                 (count (filter #(and (= :info (:type %))
                                     (#{:write :cas} (:f %))) ops))
                 :analysis (assoc result :final-paths (take 10 (:final-paths result))
                                        :configs (take 10 (:configs result)))}]
    (with-open [w (io/writer out-path)]
      (binding [*out* w] (pprint summary)))
    (spit (str out-path ".verdict") (str (pr-str verdict) "\n"))
    (println (pr-str (dissoc summary :analysis)))
    (cond (true? verdict) 0 (false? verdict) 1 :else 2)))

(defn -main [& args]
  (let [rc (try
             (case (first args)
               "keys" (do (doseq [k (validate-full (read-history (second args)))]
                            (println k)) 0)
               "check" (apply check-one (rest args))
               (throw (ex-info "Usage: keys FULL | check FULL SAVED KEY ALGORITHM OUT" {})))
             (catch Throwable t (.printStackTrace t) 2))]
    (shutdown-agents)
    (System/exit rc)))
