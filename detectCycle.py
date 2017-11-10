#!/usr/bin/env python

class DAGraph:
    def __init__(self):
        self.nodes = []
        self.adj = {}
    
    def add_node(self, vertex):
        if vertex not in self.nodes:
            self.nodes.append(vertex)
    
    def add_edge(self, u, v):
        add_node(u)
        add_node(v)
        
        if u not in self.adj:
            self.adj[u] = []
            
        if v not in self.adj[u]:
            self.adj[u].append(v)
            
    def get_nodes(self):
        return self.nodes
    
    def get_edges(self, node):
        return self.adj.get(node)
            

def is_cyclic(graph):
    nodes = graph.get_nodes()
    visited = []
    visited.append(nodes[0])
    
        