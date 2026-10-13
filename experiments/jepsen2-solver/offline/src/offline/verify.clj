(ns offline.verify
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [knossos.model :as model]
            [offline.versioned-register :as register])
  (:import (java.io PushbackReader)
           (java.nio.file Files Paths)
           (java.security MessageDigest)
           (java.util HexFormat)))

(defn read-history [path]
  (with-open [r (PushbackReader. (io/reader path))]
    (loop [ops []]
      (let [op (edn/read {:eof ::eof} r)]
        (if (= ::eof op) ops
          (do (assert (map? op)) (recur (conj ops op))))))))

(defn sha256 [path]
  (.formatHex (HexFormat/of)
    (.digest (MessageDigest/getInstance "SHA-256")
      (Files/readAllBytes (Paths/get path (make-array String 0))))))

(defn normalized [x]
  (cond (keyword? x) (name x)
        (map? x) (into {} (map (fn [[k v]] [(if (string? k) (keyword k) k) (normalized v)]) x))
        (sequential? x) (mapv normalized x)
        :else x))

(defn call [ipos cpos inv response]
  (let [status (if response (name (:type response)) "unfinished")]
    {:id (str ipos) :invoke_pos ipos :complete_pos cpos
     :invocation (normalized inv) :response (normalized response)
     :status status :f (name (:f inv))
     :model_value (if (= "ok" status) (:value response) (:value inv))}))

(defn reconstruct [full key]
  (let [{:keys [pending calls projection]}
        (reduce
          (fn [{:keys [pending calls projection] :as acc} [pos raw]]
            (let [p (:process raw)]
              (if-not (integer? p)
                (do (assert (= :nemesis p))
                    (update acc :projection conj raw))
                (let [[k v] (:value raw)
                      _ (assert (and (integer? k) (vector? v) (= 2 (count v))))
                      _ (assert (#{:invoke :ok :fail :info} (:type raw)))
                      _ (assert (#{:read :write :cas} (:f raw)))
                      event (assoc raw :value v)
                      acc (if (= k key) (update acc :projection conj event) acc)]
                  (if (= :invoke (:type raw))
                    (do (assert (not (contains? pending p)))
                        (assoc-in acc [:pending p] [pos k event]))
                    (let [[ipos ik inv] (get pending p)]
                      (assert inv "Completion without invocation")
                      (assert (= ik k))
                      (assert (= (:f inv) (:f event)))
                      (when (#{:info :fail} (:type event))
                        (assert (= (:value inv) (:value event))))
                      (cond-> (update acc :pending dissoc p)
                        (= key k) (update :calls conj (call ipos pos inv event)))))))))
          {:pending {} :calls [] :projection []}
          (map-indexed vector full))]
    {:projection projection
     :calls (into calls
                  (for [[ipos k inv] (vals pending) :when (= k key)]
                    (call ipos nil inv nil)))}))

(defn rational [text]
  (let [[a b] (str/split text #"/")]
    (/ (bigint a) (if b (bigint b) 1))))

(defn verify-one [full-path saved-path witness-path out-path]
  (let [w (edn/read-string (slurp witness-path))
        _ (assert (= 1 (:schema w)))
        _ (assert (= (sha256 full-path) (:full_sha256 w)))
        _ (assert (= (sha256 saved-path) (:independent_sha256 w)))
        full (read-history full-path)
        _ (assert (= (count full) (:full_event_count w)))
        {:keys [projection calls]} (reconstruct full (:key w))
        _ (assert (= projection (read-history saved-path)))
        _ (assert (= (normalized projection) (:events w)))
        expected (into {} (map (juxt :id identity) calls))
        decisions (:operations w)
        by-id (into {} (map (juxt :id identity) decisions))
        _ (assert (= (count decisions) (count by-id) (count expected)))
        _ (assert (= (set (keys expected)) (set (keys by-id))))
        _ (doseq [[id base] expected]
            (let [op (get by-id id) choice (:decision op)]
              (assert (= base (select-keys op (keys base))) "Original operation changed")
              (case (:status base)
                "ok" (assert (= "included" choice))
                "fail" (assert (= "upstream-fail" choice))
                (assert (#{"included" "omitted"} choice)))
              (if (= "included" choice)
                (let [t (rational (:time op))]
                  (assert (< (:invoke_pos base) t))
                  (when (= "ok" (:status base))
                    (assert (< t (:complete_pos base)))))
                (assert (not-any? #(contains? op %) [:time :state_after :model_version])))))
        order (:order w)
        included (set (map :id (filter #(= "included" (:decision %)) decisions)))
        _ (assert (= (count order) (count (set order))))
        _ (assert (= (set order) included))
        ranks (zipmap order (range))
        edges (for [a decisions b decisions
                    :when (and (= "included" (:decision a))
                               (= "included" (:decision b))
                               (= "ok" (:status a))
                               (< (:complete_pos a) (:invoke_pos b)))] [a b])
        _ (doseq [[a b] edges]
            (assert (< (ranks (:id a)) (ranks (:id b))) "Real-time order violated"))
        final
        (first
          (reduce
            (fn [[state last-time] id]
              (let [op (by-id id)
                    t (rational (:time op))
                    upstream-op {:type :invoke :f (keyword (:f op)) :value (:model_value op)}
                    next-state (model/step state upstream-op)]
                (assert (<= last-time t))
                (assert (not (model/inconsistent? next-state))
                        (str "Exact upstream model rejected " id ": " next-state))
                (assert (= (:model_version op) (:version next-state)))
                (assert (= (:state_after op)
                           (select-keys next-state [:version :value])))
                [next-state t]))
            [(register/->VersionedRegister 0 nil) -1]
            order))
        result {:valid? true :key (:key w) :operations (count decisions)
                :included (count order) :real-time-edges-checked (count edges)
                :final-state (select-keys final [:version :value])
                :witness-sha256 (sha256 witness-path)
                :model "exact upstream VersionedRegister"}]
    (spit out-path (str (pr-str result) "\n"))
    (println (pr-str result))))

(defn -main [& args]
  (let [rc (try
             (assert (and (seq args) (zero? (mod (count args) 4)))
                     "Arguments: FULL SAVED WITNESS.edn OUT.edn, repeatable")
             (doseq [group (partition 4 args)] (apply verify-one group))
             0
             (catch Throwable t (.printStackTrace t) 2))]
    (shutdown-agents)
    (System/exit rc)))
