(defproject jepsen-register-offline "0.0.0-SNAPSHOT"
  :description "Throwaway recheck of saved etcd versioned register histories"
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [knossos "0.3.15"]]
  :jvm-opts ["-Xmx10g" "-Djava.awt.headless=true"]
  :main offline.recheck)
