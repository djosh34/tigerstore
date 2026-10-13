(ns offline.selftest
  (:require [knossos.wgl :as wgl]
            [knossos.linear :as linear]
            [knossos.competition :as competition]
            [offline.versioned-register :as register]))

(defn op [process type f value]
  {:process process :type type :f f :value value})

(def cases
  [{:name "unknown write is required, not dropped"
    :valid true
    :history [(op 0 :invoke :write [nil 4])
              (op 0 :info :write [nil 4])
              (op 1 :invoke :read [nil nil])
              (op 1 :ok :read [1 4])]}
   {:name "unknown CAS is required and respects its compare operand"
    :valid true
    :history [(op 0 :invoke :write [nil 1])
              (op 0 :ok :write [1 1])
              (op 1 :invoke :cas [nil [1 4]])
              (op 1 :info :cas [nil [1 4]])
              (op 2 :invoke :read [nil nil])
              (op 2 :ok :read [2 4])]}
   {:name "unknown CAS cannot ignore the compare operand"
    :valid false
    :history [(op 0 :invoke :write [nil 1])
              (op 0 :ok :write [1 1])
              (op 1 :invoke :cas [nil [2 4]])
              (op 1 :info :cas [nil [2 4]])
              (op 2 :invoke :read [nil nil])
              (op 2 :ok :read [2 4])]}
   {:name "stale successful read after completed write is invalid"
    :valid false
    :history [(op 0 :invoke :write [nil 4])
              (op 0 :ok :write [1 4])
              (op 1 :invoke :read [nil nil])
              (op 1 :ok :read [1 3])]}
   {:name "successful writes cannot skip versions"
    :valid false
    :history [(op 0 :invoke :write [nil 4])
              (op 0 :ok :write [2 4])]}
   {:name "unknown mutations may take effect after their info event"
    :valid true
    :history [(op 0 :invoke :write [nil 4])
              (op 0 :info :write [nil 4])
              (op 1 :invoke :write [nil 3])
              (op 1 :ok :write [1 3])
              (op 2 :invoke :read [nil nil])
              (op 2 :ok :read [2 4])]}])

(defn -main [& _]
  (let [rc (try
             (doseq [[algorithm analyze] [["wgl" wgl/analysis]
                                          ["linear" linear/analysis]
                                          ["competition" competition/analysis]]
                     {:keys [name valid history]} cases]
               (let [result (analyze (register/->VersionedRegister 0 nil) history)]
                 (assert (= valid (:valid? result))
                         (str algorithm " failed " name ": " (pr-str result)))
                 (println algorithm name "PASS")))
             0
             (catch Throwable t (.printStackTrace t) 2))]
    (shutdown-agents)
    (System/exit rc)))
