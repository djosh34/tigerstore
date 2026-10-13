(defproject jepsen-register-witness "0.0.0-SNAPSHOT"
  :description "Independent replay of solver witnesses, no search"
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [knossos "0.3.15"]]
  :jvm-opts ["-Xmx512m" "-Djava.awt.headless=true"]
  :main offline.verify)
