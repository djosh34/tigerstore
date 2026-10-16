(ns jepsen.etcd.pause-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]
            [jepsen.etcd.db :as etcd]))

(deftest signal-only-the-server-pid
  (doseq [[operation signal] [[db/pause! :-STOP] [db/resume! :-CONT]]]
    (let [commands (atom [])]
      (with-redefs [c/exec (fn [& args]
                            (swap! commands conj (vec args))
                            (if (= :cat (first args)) "778\n" ""))
                    cu/grepkill! (fn [& args]
                                  (swap! commands conj (vec args)))]
        (operation (etcd/map->DB {}) {} "n1"))
      (is (= [[:cat etcd/pidfile] [:kill signal "778"]] @commands)))))

(deftest signal-errors-reach-the-nemesis
  (doseq [operation [db/pause! db/resume!]]
    (with-redefs [c/exec (fn [& args]
                          (if (= :cat (first args))
                            "778\n"
                            (throw (ex-info "signal failed" {}))))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"signal failed"
                           (operation (etcd/map->DB {}) {} "n1"))))))
