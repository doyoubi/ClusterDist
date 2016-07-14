import random
import operator
from itertools import repeat, izip_longest

class Node(object):
    def __init__(self, tag, host, master=None):
        self.tag = tag
        self.host = host
        self.master = master

class Machine(object):
    def __init__(self, tag, masters, slaves, free_nodes):
        self.tag = tag
        self.masters = masters
        self.slaves = slaves
        self.free_nodes = free_nodes

    def to_dict(self):
        return {
            'tag': self.tag,
            'masters': self.masters,
            'slaves': self.slaves,
            'frees': self.free_nodes,
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
            other_m = random.choice(masters[:m] + masters[m+1:])
        slaves[m].append(Node(i, m, random.choice(other_m)))
    return masters, slaves

def print_cluster(masters, slaves, machine_count):
    ms = [['*{}'.format(n.tag) for n in m] for m in masters]
    ss = [['#{}-{}'.format(n.tag, n.master.tag) for n in m] for m in slaves]
    all_node = map(operator.add, ms, ss)
    for line in izip_longest(*all_node, fillvalue=' '): 
        print '\t'.join(repeat('{}', machine_count)).format(*line)

machine_count = 3
ms, ss = gen_cluster(3, 5, machine_count)
print_cluster(ms, ss, machine_count)

def gen_free_nodes(machine_count, num):
    machines = list(range(machine_count))
    nodes = [list() for i in range(machine_count)]
    for i in range(num):
        m = random.choice(machines)
        nodes[m].append(Node(i, m))
    return nodes

free_nodes = gen_free_nodes(machine_count, 3)

machines = [Machine(i, ms[i], ss[i], free_nodes[i]) for i in range(machine_count)]
    
for d in map(Machine.to_dict, machines):
    print d

def dist_slave(machines, free_count):
    machines = sorted(machines, key=lambda m: len(m.masters))


dist_slave(machines, len(free_nodes))

