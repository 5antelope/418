#ifndef __VERTEX_SET__
#define __VERTEX_SET__

#include "graph.h"
#include <unordered_set>

typedef enum {
    SPARSE,
    DENSE,
    PAGERANK,
} VertexSetType;

typedef struct {
    int size;     // Number of nodes in the set
    int numNodes; // Number of nodes in the graph
    VertexSetType type;
    //int k;
    //bool* visited;
    bool* curSetFlags;
} VertexSet;

VertexSetType setType(const Graph, VertexSet*);

VertexSet *newVertexSet(VertexSetType type, int capacity, int numNodes);
void freeVertexSet(VertexSet *set);

void addVertex(VertexSet *set, Vertex v);
void removeVertex(VertexSet *set, Vertex v);

VertexSet*  vertexUnion(VertexSet *u, VertexSet* v);

#endif // __VERTEX_SET__