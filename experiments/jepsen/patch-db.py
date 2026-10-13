#!/usr/bin/env python3
"""Only adapt database installation, launch, and primary endpoint lookup."""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
p = root / 'src/jepsen/etcd/db.clj'
s = p.read_text()
s = s.replace('(client/member-status c node)', '''(-> (client/maintenance-client c)
                                (.statusMember (s/client-url node))
                                client/await client/->clj)''')
a = s.index('(defn start!')
b = s.index('(defn kill!', a)
s = s[:a] + '''(defn peer-url [node]
  (str "https://" (cn/ip node) ":2380"))

(defn start!
  "Start the selected binary with plain client endpoints and peer mTLS."
  [test node opts]
  (let [tiger? (= "tigerstore" (System/getenv "STORE"))
        peers (str/join "," (map #(str % "=" (peer-url %)) (:nodes test)))
        common [:--name node
                :--data-dir (data-dir node)
                :--listen-peer-urls (peer-url node)
                :--listen-client-urls (s/client-url node)
                :--advertise-client-urls (s/client-url node)
                :--initial-cluster peers
                :--initial-cluster-token "jepsen49"
                :--peer-cert-file "/opt/test-assets/peer.crt"
                :--peer-key-file "/opt/test-assets/peer.key"
                :--peer-trusted-ca-file "/opt/test-assets/ca.crt"]
        extra (when-not tiger?
                [:--initial-cluster-state (:initial-cluster-state opts :existing)
                 :--initial-advertise-peer-urls (peer-url node)
                 :--peer-client-cert-auth
                 :--snapshot-count (:snapshot-count test)
                 :--log-outputs :stderr :--logger :zap])]
    (c/su
      (apply cu/start-daemon!
             {:logfile logfile :pidfile pidfile :chdir dir}
             binary (concat common extra)))))

''' + s[b:]
a = s.index('    ; Install\n')
b = s.index('    (when (:lazyfs test)', a)
s = s[:a] + '''    ; Both binaries are pre-built/downloaded by the runner, never this host.
    ; Keep the process name etcd so upstream kill and pause targets are unchanged.
    (c/su
      (c/exec :mkdir :-p dir)
      (c/exec :cp "/opt/test-assets/server" (str dir "/etcd"))
      (c/exec :chmod "755" (str dir "/etcd")))

''' + s[b:]
p.write_text(s)
# Resource configuration only. No client, workload, checker or nemesis changes.
p = root / 'project.clj'
p.write_text(p.read_text().replace('"-Xmx72g"', '"-Xmx4g"'))
