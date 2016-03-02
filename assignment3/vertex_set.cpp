#include "vertex_set.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <stdio.h>
#include "mic.h"

#include <omp.h>

#define SparseRatio 0.0001

VertexSetType setType(Graph g, VertexSet* set)
{

  if((unsigned int)set->size==num_nodes(g)){
    return PAGERANK;
  }

  unsigned int outEdges = 0;
  unsigned int inEdges = 0;

  outEdges = set->size;
  inEdges= SparseRatio*num_edges(g);

  if (inEdges > outEdges) {
    return SPARSE; //top down
  }
  else {
    return DENSE; //bottom up
  }
}

/**
 * Creates an empty VertexSet with the given type and capacity.
 * numNodes is the total number of nodes in the graph.
 *
 * Student may interpret type however they wish.  It may be helpful to
 * use different representations of VertexSets under different
 * conditions, and they different conditions can be indicated by 'type'
 */
VertexSet *newVertexSet(VertexSetType type, int capacity, int numNodes)
{
  // TODO: Implement
  VertexSet *vertexSet = new VertexSet();
  vertexSet->size = 0;
  vertexSet->numNodes = numNodes;
  vertexSet->type = type;

  vertexSet->curSetFlags = (bool *)malloc(numNodes * sizeof(bool));
  #pragma omp parallel for
  for (int i=0; i<numNodes; i++) {
    vertexSet->curSetFlags[i] = false;
  }
  return vertexSet;
}

void freeVertexSet(VertexSet *set)
{
  // TODO: Implement
  free(set->curSetFlags);
  delete set;
}

void addVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  set->curSetFlags[v] = true;
  set->size+=1;
}

void removeVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  set->curSetFlags[v] = false;
  set->size-=1;
}

/**
 * Returns the union of sets u and v. Destroys u and v.
 */
VertexSet* vertexUnion(VertexSet *u, VertexSet* v)
{
  // TODO: Implement

  // STUDENTS WILL ONLY NEED TO IMPLEMENT THIS FUNCTION IN PART 3 OF
  // THE ASSIGNMENT
  VertexSet* unionSet = newVertexSet(u->type, u->numNodes, u->numNodes);

  #pragma omp parallel for schedule(static)
  for (int i=0; i<u->numNodes; i++)
  {
    if (u->curSetFlags[i]==true || v->curSetFlags[i]==true)
      unionSet->curSetFlags[i] = true;
  }

  int sum = 0;
  #pragma omp parallel for reduction(+:sum)
  for (int i=0; i<u->numNodes; i++)
  {
    if (unionSet->curSetFlags[i])
      sum += 1;
  }

  unionSet->size = sum;

  return unionSet;
}

