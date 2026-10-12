#!/usr/bin/env python3
"""Question: does scoped loopback netem install AND affect only peer packets?
Runner only: sudo unshare --net python3 experiments/chaos49/network.py
"""
import json, socket, statistics, struct, subprocess, threading, time


def cmd(*args):
    p = subprocess.run(args, check=True, text=True, capture_output=True)
    return p.stdout


def tc(*args):
    return cmd('tc', *args)


def stats():
    return json.loads(tc('-j', '-s', 'qdisc', 'show', 'dev', 'lo'))


def netstat():
    return next(s for s in stats() if s['kind'] == 'netem')


def setup(options, reverse=False):
    subprocess.run(['tc', 'qdisc', 'del', 'dev', 'lo', 'root'], capture_output=True)
    # priomap zero puts every unmatched packet in band 1, not netem band 3.
    tc('qdisc', 'add', 'dev', 'lo', 'root', 'handle', '1:', 'prio', 'bands', '3', 'priomap', *(['0']*16))
    tc('qdisc', 'add', 'dev', 'lo', 'parent', '1:3', 'handle', '30:', 'netem', 'limit', '10000', *options)
    pref = 1
    expected_keys = []
    for proto in ('udp', 'tcp'):
        directions = [('127.77.0.1', '127.77.0.2', 'dst_port')]
        if reverse:
            directions.append(('127.77.0.2', '127.77.0.1', 'src_port'))
        for src, dst, port in directions:
            tc('filter', 'add', 'dev', 'lo', 'parent', '1:', 'protocol', 'ip', 'pref', str(pref),
               'flower', 'ip_proto', proto, 'src_ip', src, 'dst_ip', dst, port, '2380', 'classid', '1:3')
            expected_keys.append({'eth_type':'ipv4', 'ip_proto':proto, 'src_ip':src, 'dst_ip':dst, port:2380})
            pref += 1
    filters = [f for f in json.loads(tc('-j', '-s', 'filter', 'show', 'dev', 'lo', 'parent', '1:')) if 'options' in f]
    assert len(filters) == pref-1, filters
    assert all(f['options']['classid'] == '1:3' for f in filters), filters
    assert [f['options']['keys'] for f in filters] == expected_keys, filters
    installed = netstat()['options']
    assert installed['limit'] == 10000, installed
    for name in ('delay', 'loss', 'corrupt', 'reorder', 'gap'):
        if name not in options: continue
        value = options[options.index(name)+1]
        if name == 'delay': assert installed['delay']['delay'] == float(value[:-2])/1000, installed
        elif name == 'loss': assert installed['loss-random']['loss'] == float(value[:-1])/100, installed
        elif name == 'gap': assert installed['gap'] == int(value), installed
        else: assert installed[name][name] == float(value[:-1])/100, installed
    print(json.dumps({'installed': options, 'reverse': reverse, 'qdisc': stats(), 'filters': filters}), flush=True)


def probe_burst():
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4*1024*1024)
    receiver.bind(('127.77.0.2', 2380))
    receiver.settimeout(.2)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender.bind(('127.77.0.1', 0))
    # Disable UDP checksum for the corruption canary only, not product traffic.
    sender.setsockopt(socket.SOL_SOCKET, 11, 1) # Linux SO_NO_CHECK
    arrivals = []
    done = threading.Event()
    def receive():
        while not done.is_set():
            try:
                data, _ = receiver.recvfrom(4096)
                arrivals.append((time.monotonic_ns(), data))
            except socket.timeout:
                pass
    t = threading.Thread(target=receive)
    t.start()
    sent = {}
    for i in range(300):
        sent[i] = time.monotonic_ns()
        sender.sendto(struct.pack('!I', i)+bytes([i%251])*996, ('127.77.0.2',2380))
        time.sleep(.002)
    time.sleep(.2)
    done.set(); t.join(); receiver.close(); sender.close()
    bad = reorder = 0
    last = -1
    latency = []
    for at, data in arrivals:
        i = struct.unpack('!I', data[:4])[0]
        if i not in sent or data != struct.pack('!I',i)+bytes([i%251])*996:
            bad += 1
            continue
        reorder += i < last
        last = max(last, i)
        latency.append((at-sent[i])/1e6)
    return {'sent':300, 'received':len(arrivals), 'corrupted':bad, 'out_of_order':reorder,
            'one_way_median_ms':statistics.median(latency), 'qdisc':netstat()}


def echo_rtt(proto, port, source):
    kind = socket.SOCK_STREAM if proto == 'tcp' else socket.SOCK_DGRAM
    listener = socket.socket(socket.AF_INET, kind)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.77.0.2',port))
    if proto == 'tcp': listener.listen()
    def serve():
        if proto == 'tcp':
            conn,_=listener.accept()
            with conn:
                for _ in range(20):
                    b=conn.recv(32); conn.sendall(b)
        else:
            for _ in range(20):
                b,a=listener.recvfrom(32); listener.sendto(b,a)
    t=threading.Thread(target=serve); t.start()
    s=socket.socket(socket.AF_INET,kind); s.settimeout(2); s.bind((source,0)); s.connect(('127.77.0.2',port))
    times=[]
    for _ in range(20):
        now=time.monotonic_ns(); s.send(b'x'); assert s.recv(32)==b'x'; times.append((time.monotonic_ns()-now)/1e6)
    s.close(); t.join(); listener.close()
    # Let TCP teardown finish before resetting qdisc counters for the next case.
    if proto == 'tcp': time.sleep(.1)
    return statistics.median(times)


cmd('ip','link','set','lo','up')
print(cmd('uname','-a'), cmd('tc','-V'), cmd('python3','--version'), flush=True)
results={}
for name, options, reverse in [
    ('baseline',[],False), ('delay_one_way',['delay','5ms'],False),
    ('delay_both',['delay','5ms'],True), ('loss',['loss','25%'],False),
    ('reorder',['delay','10ms','reorder','100%','gap','2'],False),
    ('corrupt',['corrupt','10%'],False)]:
    setup(options,reverse)
    result=probe_burst()
    before=netstat()['packets']
    # Same node IP pair but client port, then a client IP to the peer port.
    result['client_port_rtt_ms']=echo_rtt('udp',2379,'127.77.0.1')
    result['client_ip_rtt_ms']=echo_rtt('tcp',2380,'127.77.0.9')
    assert netstat()['packets']==before, 'control traffic entered netem'
    if name in ('baseline','delay_one_way','delay_both'):
        result['peer_tcp_rtt_ms']=echo_rtt('tcp',2380,'127.77.0.1')
    if name=='loss': assert 30 < 300-result['received'] < 130, result
    if name=='reorder': assert result['out_of_order']>20, result
    if name=='corrupt': assert result['corrupted']>5, result
    if name.startswith('delay'): assert 4 < result['one_way_median_ms'] < 20, result
    assert result['qdisc']['packets']+result['qdisc']['drops'] >= 290, result
    results[name]=result
    print(json.dumps({'case':name, **result}),flush=True)
assert results['delay_one_way']['peer_tcp_rtt_ms']>4
assert results['delay_both']['peer_tcp_rtt_ms']>9
print('PASS: install, exercised fault, and unaffected control paths verified',flush=True)
