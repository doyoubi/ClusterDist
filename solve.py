import random
import operator
from datetime import datetime
from itertools import repeat, izip_longest
from collections import defaultdict
from itertools import imap
from copy import deepcopy

from igraph import Graph

class Node(object):
    def __init__(self, tag, machine_tag, master=None):
        self.tag = tag
        self.machine_tag = machine_tag
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
    print '-' * 50
    ms = [['*{}'.format(n.tag) for n in m] for m in masters]
    ss = [['#{}-{}'.format(n.tag, n.master.tag) for n in m] for m in slaves]
    fs = [['f{}'.format(f.tag) for f in m] for m in frees]
    all_node = map(operator.add, ms, ss)
    all_node = map(operator.add, all_node, fs)
    for line in izip_longest(*all_node, fillvalue=' '): 
        print '\t'.join(repeat('{}', machine_count)).format(*line)
    print '-' * 50

machine_count = 3
master_count = 10
slave_count = 10
free_count = 10
try_times = 100000
ms, ss = gen_cluster(master_count, slave_count, machine_count)

def gen_free_nodes(machine_count, master_slave_sum, num):
    machines = list(range(machine_count))
    nodes = [list() for i in range(machine_count)]
    for i in range(num):
        m = random.choice(machines)
        nodes[m].append(Node(master_slave_sum + i, m))
    return nodes

fs = gen_free_nodes(machine_count, master_count + slave_count, free_count)

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
    ct = machine_count
    g = Graph().as_directed()
    g.add_vertices(2 * ct + 2)
    g.es['weight'] = 1
    assert g.is_weighted()
    lin = map(len, fs)
    # lin = map(operator.add, map(len, ss), map(len, fs))
    rout = map(len, ms)
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
    # for i in range(ct):
    #     for slave in ss[i]:
    #         cap[i][slave.master.machine_tag] -= 1
    #         assert cap[i][slave.master.machine_tag] >= 0
    print cap
    for i in range(ct):
        for j in range(ct):
            if i == j:
                continue
            masters_in_j = set(slave.master.tag for slave in ss[i] if slave.master.machine_tag == j)  # to fight existing distribution error
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


class SolverError(Exception):
    pass

class MaxFlowSolver(object):
    def __init__(self, machines, masters, slaves, frees, ms, ss, fs):
        self.machines = machines
        self.masters = masters
        self.slaves = slaves
        self.frees = frees
        self.orphans = [m for m in masters if len(m.slaves) == 0]

        self.ms = ms
        self.ss = ss
        self.fs = fs
        self.os = [[m for m in machine if len(m.slaves) == 0] for machine in ms]

        self.vertex_count = 2 * len(machines) + 2
        self.s = self.vertex_count - 2
        self.t = self.vertex_count - 1
        self.result = []
        self.max_flow = 0
        self.init_slice_tag()

    def _gen_graph(self):
        g = Graph().as_directed()
        g.add_vertices(self.vertex_count)
        g.es['weight'] = 1  # enable weight
        assert g.is_weighted()
        return g

    def init_slice_tag(self):
        for m in self.machines:
            for master in m.masters:
                m.slice_tags.append(master.tag)
            for slave in m.slaves:
                m.slice_tags.append(slave.master.tag)

    def fill_all(self):
        try:
            self.fill_remaining(True)
        except SolverError:
            print 'fill all: all masters have slaves not satisfied !!!!!!!'
 
    def fill_orphans(self):
        g = self._gen_graph()
        ct = len(self.machines)
        flow_in = map(len, self.fs)
        flow_out = map(len, self.os)  # set it to orphans first
        for i, c in enumerate(flow_in):
            g[self.s, i] = c
        for i, c in enumerate(flow_out):
            g[i + ct, self.t] = c
        for i in xrange(ct):
            for j in xrange(ct):
                if i == j:
                    continue
                g[i, ct + j] = len(self.fs[i])

        print [(e.source, e.target) for e in g.es]
        print g.es['weight']
        mf = g.maxflow(self.s, self.t, g.es['weight'])
        if mf.value < len(self.orphans):
            raise SolverError('unable to distribute slaves')
        print 'max flow value: ', mf.value
        self.max_flow += mf.value
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.fs[e.source].pop()
                o = self.os[e.target - ct].pop()
                f.master = o
                o.slaves.append(f)
                self.ss[e.source].append(f)
                self.machines[e.source].slice_tags.append(o.tag)
                self.result.append((f.tag, o.tag))
        assert sum(map(len, self.os)) == 0

    def fill_remaining(self, fill_all=False):
        g = self._gen_graph()
        ct = len(self.machines)
        flow_in = map(len, self.fs)
        flow_out = map(len, self.ms)
        flow_out = list(repeat(50, len(self.ms)))  # warning
        for i, c in enumerate(flow_in):
            g[self.s, i] = c
        for i, c in enumerate(flow_out):
            g[i + ct, self.t] = c
        for i in xrange(ct):
            for j in xrange(ct):
                if i == j:
                    continue
                masters_in_j = set(slave.master.tag for slave in self.ss[i] if slave.master.machine_tag == j)  # to fight existing distribution error
                limit = len(self.ms[j]) - len(masters_in_j)
                g[i, ct + j] = min(len(self.fs[i]), limit)
                print i, j, g[i, ct + j]
        mf = g.maxflow(self.s, self.t, g.es['weight'])
        print 'max flow value: ', mf.value
        self.max_flow += mf.value

        if fill_all and not self._all_have_slaves(g, mf):
            raise SolverError('not all masters have slaves')

        self._sort_masters()
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            for _ in xrange(int(mf.flow[edge_index])):
                f = self.fs[e.source].pop()
                m = next((m for m in self.ms[e.target - ct] if m.tag not in self.machines[e.source].slice_tags), None)
                assert m is not None
                # if m is None:
                #     self.fs[e.source].append(f)
                #     print 'conflict: ', e.source, e.target, f.tag, int(mf.flow[edge_index]), self.machines[e.source].slice_tags
                #     break
                f.master = m
                m.slaves.append(f)
                self.ss[e.source].append(f)
                self.result.append((f.tag, m.tag))
                self.machines[e.source].slice_tags.append(m.tag)
                self.ms[e.target - ct].sort(key=lambda m: len(m.slaves))

        assert all(len(m.slaves) > 0 for m in sum(self.ms, []))

    def _sort_masters(self):
        for masters in self.ms:
            masters.sort(key=lambda m: len(m.slaves))

    def _all_have_slaves(self, g, mf):
        ct = len(self.machines)
        flows = list(repeat(0, ct))
        for edge_index, e in enumerate(g.es):
            if e.source == self.s or e.target == self.t:
                continue
            flows[e.target - ct] += int(mf.flow[edge_index])
        print flows
        print map(len, self.os)
        return all(map(operator.ge, flows, map(len, self.os)))


cms = deepcopy(ms)
css = deepcopy(ss)
cfs = deepcopy(fs)
cmachines = deepcopy(machines)
try:
    solver = MaxFlowSolver(cmachines, sum(cms, []), sum(css, []), sum(cfs, []), cms, css, cfs)
    print 'orphans: ', [o.tag for o in solver.orphans]
    solver.fill_orphans()
    print_cluster(cms, css, cfs, machine_count)
    print solver.result
    solver.fill_remaining()
    print_cluster(cms, css, cfs, machine_count)
    print solver.result
    print 'max flow: ', solver.max_flow
except SolverError as e:
    print '#' * 50
    print e.message
    print '#' * 50
    solver.max_flow = 0

def check():
    for m in fs:
        for f in m:
            assert f.master is None
    # assert len(sum(css, [])) > len(sum(ss, []))
check()

print '*' * 100
all_solver = MaxFlowSolver(machines, sum(ms, []), sum(ss, []), sum(fs, []), ms, ss, fs)
all_solver.fill_all()
print_cluster(ms, ss, fs, machine_count)
print all_solver.result
print 'max flow: ', solver.max_flow, all_solver.max_flow


