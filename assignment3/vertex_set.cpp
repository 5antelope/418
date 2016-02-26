#include "vertex_set.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <stdio.h>
#include "mic.h"

#include <omp.h>

VertexSetType setType(Graph g, VertexSet* set)
{
  int numNodes = num_nodes(g);
  int setNodes = set->size;
  int outEdges = 0;
  int inEdges = 0;
  int numEdges = num_edges(g);

  #pragma omp parallel for reduction(+:outEdges) schedule(static)
  for (int i=0; i<set->numNodes; i++)
  {
    if (set->curSetFlags[i]==1)
      outEdges += outgoing_size(g, i);
  }

  inEdges = (numEdges/numNodes) * setNodes / 2;

  // if (setNodes/numNodes > outEdges/numEdges)
  if (inEdges > outEdges)
      return SPARSE;
  else
      return DENSE;
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

  vertexSet->vertices = NULL;

  vertexSet->curSetFlags = (int *)malloc(numNodes * sizeof(int));
  #pragma omp parallel for
  for (int i=0; i<numNodes; i++)
    vertexSet->curSetFlags[i] = 0;

  return vertexSet;
}

void freeVertexSet(VertexSet *set)
{
  // TODO: Implement
  if (set->vertices != NULL)
    free(set->vertices);
  free(set->curSetFlags);
  delete set;
}

void addVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  set->curSetFlags[v] = 1;
}

void removeVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  set->curSetFlags[v] = 0;
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
    if (u->curSetFlags[i]==1)
        unionSet->curSetFlags[i] = 1;
  }

  #pragma omp parallel for schedule(static)
  for (int i=0; i<v->numNodes; i++)
  {
    if (v->curSetFlags[i]==1)
        unionSet->curSetFlags[i] = 1;
  }

  int sum = 0;
  #pragma omp parallel for reduction(+:sum)
  for (int i=0; i<u->numNodes; i++)
    sum += unionSet->curSetFlags[i];

  unionSet->size = sum;

  return unionSet;
}

