"""sysrun.py dir: syscalls, syscall bytes, kernel CPU profile and kernel copies of the sys run."""
import json, os, re, sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from bpfmap import parse
from sys_names import NR

d = sys.argv[1]
roles = open(os.path.join(d, "roles.txt")).read().split("\n")
lp = roles[0].split()[2]
fp = roles[1].split()[2]
res = json.load(open(os.path.join(d, "result.json")))
ops = res["ops"]
print(f"ops {ops}, {res['throughput']:.0f} ops/s, cpu/op by node {res['server_cpu_us_per_op_by_node']}, leader pid {lp}, follower pid {fp}")

def who(p):
    return {lp: "leader", fp: "follower"}.get(p, p)

# syscalls by name
sc = defaultdict(lambda: defaultdict(int))
for keys, _, v in parse(os.path.join(d, "sys.txt"), "syscalls"):
    sc[who(keys[0])][int(keys[1])] += v
sb = defaultdict(lambda: defaultdict(int))
for keys, _, v in parse(os.path.join(d, "sys.txt"), "sysbytes"):
    sb[who(keys[0])][int(keys[1])] += v
print("\n| syscall | leader /op | leader bytes/call | follower /op | follower bytes/call |\n|---|---:|---:|---:|---:|")
for nr in sorted(set(sc["leader"]) | set(sc["follower"]), key=lambda n: -(sc["leader"][n] + sc["follower"][n])):
    a, b = sc["leader"][nr], sc["follower"][nr]
    if (a + b) / ops < 0.002:
        continue
    ba = f"{sb['leader'][nr]/a:.0f}" if a and nr in (63, 64, 66, 67, 68, 207, 211, 212) else ""
    bb = f"{sb['follower'][nr]/b:.0f}" if b and nr in (63, 64, 66, 67, 68, 207, 211, 212) else ""
    print(f"| {NR.get(nr, nr)} | {a/ops:.3f} | {ba} | {b/ops:.3f} | {bb} |")
print(f"| all | {sum(sc['leader'].values())/ops:.3f} | | {sum(sc['follower'].values())/ops:.3f} | |")

# syscalls by layer (user stack)
def sys_layer(fr):
    s = " ".join(fr)
    if "syncGroups" in s or "wal." in s or "persist" in s or "osfs." in s:
        return "WAL/disk"
    if "peer." in s:
        return "peer"
    if "server." in s or "crypto/tls" in s:
        return "client"
    if "runtime." in s:
        return "Go runtime (futex, epoll, sleep, madvise)"
    return "other"
L = defaultdict(lambda: defaultdict(int))
for keys, fr, v in parse(os.path.join(d, "sys.txt"), "sysstack"):
    L[who(keys[0])][(sys_layer(fr), NR.get(int(keys[1]), keys[1]))] += v
print("\n| layer, syscall | leader /op | follower /op |\n|---|---:|---:|")
for k in sorted(set(L["leader"]) | set(L["follower"]), key=lambda k: -(L["leader"][k] + L["follower"][k])):
    a, b = L["leader"][k] / ops, L["follower"][k] / ops
    if a + b >= 0.003:
        print(f"| {k[0]}: {k[1]} | {a:.3f} | {b:.3f} |")

# kernel CPU profile
def kclass(fr):
    s = " ".join(fr)
    if not fr:
        return "user space"
    if "do_idle" in s or "cpu_startup_entry" in s or "default_idle" in s or "arch_cpu_idle" in s:
        return "idle"
    if "net_rx_action" in s or "__do_softirq" in s or "handle_softirqs" in s or "do_softirq" in s:
        if "tcp_v4_rcv" in s or "ip_rcv" in s or "process_backlog" in s or "__netif_receive_skb" in s:
            return "kernel: loopback receive (softirq: IP/TCP rx for the peer socket)"
        return "kernel: softirq other"
    if "do_page_fault" in s or "do_mem_abort" in s or "handle_mm_fault" in s or "el0_da" in s:
        return "kernel: page faults"
    if "fdatasync" in s or "fsync" in s or "ext4_sync" in s or "jbd2" in s or "blk_" in s:
        return "kernel: fdatasync / block"
    if "ext4_" in s or "generic_perform_write" in s or "vfs_write" in s and "sock" not in s:
        return "kernel: file write (page cache)"
    if "tcp_sendmsg" in s or "sock_sendmsg" in s or "sock_write_iter" in s:
        return "kernel: TCP send (incl. inline loopback delivery)"
    if "tcp_recvmsg" in s or "sock_recvmsg" in s or "sock_read_iter" in s:
        return "kernel: TCP receive"
    if "futex" in s:
        return "kernel: futex"
    if "epoll" in s:
        return "kernel: epoll"
    if "schedule" in s or "__switch_to" in s or "finish_task_switch" in s or "nanosleep" in s:
        return "kernel: scheduler, context switch, nanosleep"
    if "madvise" in s or "mmap" in s or "munmap" in s:
        return "kernel: mmap/madvise"
    if "el0_svc" in s or "invoke_syscall" in s:
        return "kernel: other syscalls"
    if "gic_handle_irq" in s or "el1_interrupt" in s or "el0_interrupt" in s or "irq" in s:
        return "kernel: interrupts, timers"
    return "kernel: other"
P = defaultdict(lambda: defaultdict(int))
tot = defaultdict(int)
for keys, fr, v in parse(os.path.join(d, "sys.txt"), "kprof"):
    cpu, pid = keys[0], keys[1]
    w = who(pid) if pid in (lp, fp) else ("idle" if pid == "0" else "other process")
    k = kclass(fr)
    if w == "idle":
        k = "idle"
    P[cpu][(w, k)] += v
    tot[cpu] += v
for cpu in sorted(P):
    print(f"\n#### CPU {cpu}: {tot[cpu]} samples at 499 Hz")
    for k, v in sorted(P[cpu].items(), key=lambda x: -x[1]):
        if v / tot[cpu] >= 0.003:
            print(f"| {k[0]} | {k[1]} | {100*v/tot[cpu]:.1f}% |")

# kernel copies
for m, label in (("k2u", "kernel->user"), ("u2k", "user->kernel")):
    p = os.path.join(d, "kcopy.txt")
    if not os.path.exists(p):
        continue
    C = defaultdict(lambda: defaultdict(int))
    for keys, fr, v in parse(p, m):
        s = " ".join(fr)
        c = "socket" if ("sock" in s or "tcp" in s) else ("file" if ("ext4" in s or "generic_perform_write" in s or "filemap" in s) else "other")
        C[who(keys[0])][c] += v
    for w in ("leader", "follower"):
        print(f"{label} {w}: " + ", ".join(f"{c} {v/ops:.0f} B/op" for c, v in sorted(C[w].items(), key=lambda x: -x[1])))
for m in ("minflt", "majflt", "k2ucalls", "u2kcalls"):
    p = os.path.join(d, "kcopy.txt")
    if os.path.exists(p):
        for keys, _, v in parse(p, m):
            print(m, who(keys[0]), f"{v/ops:.3f}/op")
