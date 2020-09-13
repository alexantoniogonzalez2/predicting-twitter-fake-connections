# %%
import sys

import pandas as pd
import math
from collections import defaultdict

# %%
from prioritydict import PriorityDict

train_data = {}
search_table = []
for line in open("data/train.txt"):
    r = line.split()
    search_table.append(r)
    sink_ls = r[1:]
    train_data[r[0]] = sink_ls


# %%


def dijkstra(data, source, sink):
    visit = {}
    dis_dict = defaultdict(lambda: math.inf)
    dis_dict[source] = 0
    expand_queue = PriorityDict()
    expand_queue[source] = 0
    # distance = 0
    while len(expand_queue) > 0:
        source = expand_queue.smallest()
        i = expand_queue[source]
        # if i != distance:
        #     print("distance", i)
        # distance = i
        expand_queue.pop_smallest()
        visit[source] = i
        if source == sink:
            return i
        if source in data:
            # print("source", source, " in data", "neighbours", len(data[source]))
            for s in data.get(source):
                if s not in visit or visit[s] > i + 1:
                    if s not in expand_queue or i + 1 < expand_queue[s]:
                        expand_queue[s] = i + 1

    return math.inf


# %%

# test_0 = {"1": ["2", "3"], "2": ["5", "4"], "4": ["5", "6"]}
# print(dijkstra(test_0, "1", "2"))

# %%

# dijkstra(train_data, '4156257', '4504242')

# %%

real_edges = pd.read_csv("model_data/real_edges.csv", sep='\t')
fake_edges = pd.read_csv("model_data/fake_edges.csv", sep='\t')
test_data = pd.read_csv("model_data/test_data.csv", sep='\t')


# %%

def worker(message_q, results, x):
    i, row = x
    distance = dijkstra(train_data, str(row['Source']), str(row['Sink']))
    message_q.put("{} {} {} {}".format(i, str(row['Source']), str(row['Sink']), distance))
    results[i] = distance


# %%

def calculate_dis(x):
    d = dijkstra(train_data, str(x.Source), str(x.Sink))
    # print(d)
    return d


# %%

real_edges['Distance'] = 1


def listener(q):
    '''listens for messages on the q, writes to file. '''

    with open("preprocess.log", 'w') as f:
        while 1:
            m = q.get()
            if m == 'kill':
                print("listener killed")
                break
            f.write(str(m) + '\n')
            print(m)
            f.flush()


# %%

from multiprocessing import Pool, Manager, cpu_count

manager = Manager()
results_f = manager.dict()
results_t = manager.dict()
message_queue = manager.Queue()
pool = Pool(cpu_count() + 2)

watcher = pool.apply_async(listener, (message_queue,))

message_queue.put("=== fake_edges ===")
pool.starmap(worker, map(lambda row: (message_queue, results_f, row), fake_edges.iterrows()))
# pool.starmap(worker, map(lambda row: (message_queue, results_f, row), [(0, {"Source": 1, "Sink": 2})]))

message_queue.put("")
message_queue.put("=== test_data ===")
pool.starmap(worker, map(lambda row: (message_queue, results_t, row), test_data.iterrows()))
# pool.starmap(worker, map(lambda row: (message_queue, results_t, row), [(0, {"Source": 1, "Sink": 2})]))

message_queue.put("kill")

# %%

dis_f = []
dis_t = []
for i in range(2000):
    dis_f.append(results_f.get(i))
    dis_t.append(results_t.get(i))

fake_edges['Distance'] = dis_f
test_data['Distance'] = dis_t

# %%

real_edges.to_csv("model_data/real_edges.csv", sep='\t', index=False)
fake_edges.to_csv("model_data/fake_edges.csv", sep='\t', index=False)
test_data.to_csv("model_data/test_data.csv", sep='\t', index=False)

# %%
