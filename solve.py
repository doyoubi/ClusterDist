import random
import operator
from datetime import datetime
from itertools import repeat, izip_longest
from collections import defaultdict

from igraph import Graph

class Node(object):
    def __init__(self, tag, host, master=None):
        self.tag = tag
        self.host = host
        self.master = master
        self.slaves = []

class Machine(object):
    def __init__(self, tag, masters, slaves, free_nodes):
        self.tag = tag
        self.masters = masters
        self.slaves = slaves
        self.frees= free_nodes
        self.slice_tags = []

    def to_dict(self):
        return {
            'tag': self.tag,
            'masters': self.masters,
            'slaves': self.slaves,
            'frees': self.frees,
        }

def gen_cluster(master_count, slave_count, machine_count):
    machines = list(range(machine_count))
    masters = [list() for i in range(machine_count)]
    slaves = [list() for i in range(machine_count)]
    for i in range(master_count):
        m = random.choice(machines)
        masters[m].append(Node(i, m))
    for i in range(master_count, master_count + slave_count):
        m = random.choice(machines)
        other_m = []
        while len(other_m) == 0:
            print 'try try'
            other_m = random.choice(masters[:m] + masters[m+1:])
        s = Node(i, m, random.choice(other_m))
        s.master.slaves.append(s)
        slaves[m].append(s)
    return masters, slaves

def print_cluster(masters, slaves, frees, machine_count):
    ms = [['*{}'.format(n.tag) for n in m] for m in masters]
    ss = [['#{}-{}'.format(n.tag, n.master.tag) for n in m] for m in slaves]
    fs = [['f' for _ in m] for m in frees]
    all_node = map(operator.add, ms, ss)
    all_node = map(operator.add, all_node, fs)
    for line in izip_longest(*all_node, fillvalue=' '): 
        print '\t'.join(repeat('{}', machine_count)).format(*line)

machine_count = 3
master_count = 10
slave_count = 10
free_count = 3
try_times = 100000
ms, ss = gen_cluster(master_count, slave_count, machine_count)

def gen_free_nodes(machine_count, num):
    machines = list(range(machine_count))
    nodes = [list() for i in range(machine_count)]
    for i in range(num):
        m = random.choice(machines)
        nodes[m].append(Node(i, m))
    return nodes

fs = gen_free_nodes(machine_count, free_count)

print_cluster(ms, ss, fs, machine_count)

machines = [Machine(i, ms[i], ss[i], fs[i]) for i in range(machine_count)]
    
# for d in map(Machine.to_dict, machines):
#     print d

def dist_slave(machines, ms, fs):
    ms = sum(ms, [])
    fs = sum(fs, [])
    init_slice_tag(machines)
    res = recur_dist(sorted_masters(ms), fs, 0)
    return res

def sorted_masters(ms):
    return sorted(ms, key=lambda m: len(m.slaves))

def check_all_masters_has_slaves(ms):
    return all(map(lambda m: len(m.slaves) > 0, ms))

tried = 0
def recur_dist(ms, fs, curr_f):
    if curr_f == len(fs):
        return check_all_masters_has_slaves(ms)
    global tried
    if tried > try_times:
        raise Exception('exceed try times')
    tried += 1
    f = fs[curr_f]
    for m in ms: 
        if m.tag in machines[f.host].slice_tags:
            continue
        f.master = m
        m.slaves.append(f)
        machines[f.host].slice_tags.append(m.tag)
        res = recur_dist(sorted_masters(ms), fs, curr_f + 1)
        if res:
            return True
        f.master = None
        m.slaves.pop()
        machines[f.host].slice_tags.pop()
    return False

def init_slice_tag(machines):
    for m in machines:
        for master in m.masters:
            m.slice_tags.append(master.tag)
        for slave in m.slaves:
            m.slice_tags.append(slave.master.tag)

def go_search():
    start = datetime.utcnow()
    try:
        res =  dist_slave(machines, ms, fs)  # go
    except:
        print 'exceed try times'
        res = False
    end = datetime.utcnow()
    print 'result:', res, end - start
    if res:
        for f in sum(fs, []):
            ss[f.host].append(f)
        print_cluster(ms, ss, repeat([], machine_count), machine_count)

def gen_graph():
    masters = sum(ms, [])
    slaves = sum(ss, [])
    frees = sum(fs, [])
    g = Graph().as_directed()
    g.add_vertices(len(masters) + len(slaves) + len(frees) + 2)
    g.es['weight'] = 1
    assert g.is_weighted()
    ct = machine_count
    # lin = map(len, fs)
    lin = map(operator.add, map(len, ss), map(len, fs))
    rout = map(len, fs)
    s = 2 * ct
    t = s + 1
    for i, m in enumerate(lin):
        g[s, i] = m
    for i, m in enumerate(rout):
        g[i + ct, t] = m

    cap = defaultdict(dict)
    for i, m in enumerate(lin):
        for j, n in enumerate(rout):
            if i == j:
                continue
            cap[i][j] = m
    assert len(ss) == ct
    for i in range(ct):
        for slave in ss[i]:
            cap[i][slave.master.host] -= 1
            assert cap[i][slave.master.host] >= 0
    print cap
    for i in range(ct):
        for j in range(ct):
            if i == j:
                continue
            masters_in_j = set(slave.master.tag for slave in ss[i] if slave.master.host == j)  # to fight existing distribution error
            limit = len(ms[j]) - len(masters_in_j)
            cap[i][j] = min(limit, map(len, fs)[i])

    for i, m in enumerate(lin):
        if m == 0:
            continue
        for j, n in enumerate(rout):
            if i == j:
                continue
            g[i, ct + j] = cap[i][j]

    print g
    start = datetime.utcnow()
    mf = g.maxflow(s, t, g.es['weight'])
    end = datetime.utcnow()
    print 'result:', end - start
    print mf.value, sum(rout)
    print mf.flow
    print map(float, g.es['weight']), len(g.es['weight'])

gen_graph()
    

    
