from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict
from collections import defaultdict, deque

app = FastAPI()

# ---- CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- MODELS ----

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[str]
    edges: List[Edge]

# ---- DAG CHECK (Kahn's Algorithm) ----

def is_dag(nodes: List[str], edges: List[Edge]) -> bool:
    # Build graph
    graph: Dict[str, List[str]] = defaultdict(list)
    indegree = {node: 0 for node in nodes}

    for e in edges:
        # Ignore invalid edges safely
        if e.source not in indegree or e.target not in indegree:
            continue

        graph[e.source].append(e.target)
        indegree[e.target] += 1

    # Queue all nodes with 0 indegree
    queue = deque([n for n in nodes if indegree[n] == 0])
    visited_count = 0

    while queue:
        node = queue.popleft()
        visited_count += 1

        for neighbor in graph[node]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)

    # If all nodes visited, it's a DAG
    return visited_count == len(nodes)

# ---- API ----

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    num_nodes = len(pipeline.nodes)
    num_edges = len(pipeline.edges)
    dag_result = is_dag(pipeline.nodes, pipeline.edges)

    print("\n---- PIPELINE RECEIVED ----")
    print("Nodes:", pipeline.nodes)
    print("Edges:", [f"{e.source} -> {e.target}" for e in pipeline.edges])
    print("Is DAG:", dag_result)
    print("---------------------------\n")

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": dag_result
    }
