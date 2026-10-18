#!/usr/bin/env python3
"""Adapt DB setup, heap and independent register checker scheduling/algorithm."""
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
                :--initial-advertise-peer-urls (peer-url node)
                :--peer-client-cert-auth
                :--listen-client-urls (s/client-url node)
                :--advertise-client-urls (s/client-url node)
                :--initial-cluster peers
                :--initial-cluster-token "jepsen49"
                :--peer-cert-file "/opt/test-assets/peer.crt"
                :--peer-key-file "/opt/test-assets/peer.key"
                :--peer-trusted-ca-file "/opt/test-assets/ca.crt"]
        extra (when-not tiger?
                [:--initial-cluster-state (:initial-cluster-state opts :existing)
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
# run.sh builds the patched LazyFS on each node before this setup. Keep
# upstream mounting and faults.
s = s.replace('(db/setup! (lazyfs node) test node)',
              '(lazyfs/mount! (:lazyfs (lazyfs node)))')
s = s.replace('(client/await-node-ready c)', '''(util/timeout 60000
          (throw (ex-info "MemberList readiness timed out" {:node node}))
          (client/await-node-ready c))
        ; MemberList can succeed before the first leader election. SetClient's
        ; setup writes without retrying. Readiness must include consensus.
        (let [deadline (+ (System/nanoTime) 30000000000)]
          (loop []
            (when-not (try (client/get* c "__jepsen_db_readiness") true
                           (catch Exception e
                             (if (< (System/nanoTime) deadline) false (throw e))))
              (Thread/sleep 100)
              (recur))))''')
# Broad pgrep can select a sibling pipeline process before it execs xargs.
# Stopping that process blocks the nemesis before it can issue resume (#217).
for signal, flag in [('stop', 'STOP'), ('cont', 'CONT')]:
    old = f'(cu/grepkill! :{signal} "etcd")'
    if s.count(old) != 1:
        raise ValueError(f'expected one {signal} command')
    s = s.replace(old, f'(c/exec :kill :-{flag} (str/trim (c/exec :cat pidfile)))')
p.write_text(s)
# Keep the versioned register model and every operation unchanged. Run only
# one independent key checker at once, with Knossos WGL instead of competition.
p = root / 'project.clj'
p.write_text(p.read_text().replace('"-Xmx72g"', '"-Xmx10g"'))
p = root / 'src/jepsen/etcd/register.clj'
s = p.read_text()
old = '''(checker/linearizable
                                 {:model (->VersionedRegister 0 nil)})'''
assert s.count(old) == 1
s = s.replace(old, '''(checker/concurrency-limit 1
                                 (checker/linearizable
                                   {:algorithm :wgl
                                    :model (->VersionedRegister 0 nil)}))''')
p.write_text(s)
