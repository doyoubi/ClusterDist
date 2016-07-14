import random
import operator
from itertools import repeat, izip_longest

class Node(object):
    def __init__(self, tag, host, port, master=None):
        self.tag = tag
        self.host = host
        self.port = port
        self.master = master


def gen_cluster(master_count, slave_count, machine_count):
    machines = list(range(machine_count))
    masters = [list() for i in range(machine_count)]
    slaves = [list() for i in range(machine_count)]
    for i in range(master_count):
        m = random.choice(machines)
        masters[m].append(i)
    for i in range(master_count, master_count + slave_count):
        m = random.choice(machines)
        slaves[m].append(i)
    return masters, slaves

def print_cluster(masters, slaves, machine_count):
    ms = [['*{}'.format(n) for n in m] for m in masters]
    ss = [['#{}'.format(n) for n in m] for m in slaves]
    all_node = map(operator.add, ms, ss)
    for line in izip_longest(*all_node, fillvalue=' '): 
        print '\t'.join(repeat('{}', machine_count)).format(*line)

ms, ss = gen_cluster(3, 5, 3)
print_cluster(ms, ss, 3)


def dist(masters, slaves, add_master, add_slaves):
    pass

