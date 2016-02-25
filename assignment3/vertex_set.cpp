#include "vertex_set.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <stdio.h>
#include "mic.h"

#include <omp.h>

enum VertexSetType setType(Graph g, VertexSet* set)
{
  int numNodes = num_nodes(g);
  int outEdges = 0;
  #pragma omp parallel for reduction(+:outEdges) schedule(static)
  for (int i=0; i<set->numNodes; i++)
  {
    if (set->curSetFlags[i]==1)
      outEdges += outgoing_size(g, i);
  }

  if (numNodes > outEdges)
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

  vertexSet->vertices = (Vertex *)malloc(capacity * sizeof(Vertex));

  vertexSet->curSetFlags = (int *)malloc(numNodes * sizeof(int));
  #pragma omp parallel for
  for (int i=0; i<numNodes; i++)
    vertexSet->curSetFlags[i] = 0;

  return vertexSet;
}

void freeVertexSet(VertexSet *set)
{
  // TODO: Implement
  free(set->vertices);
  delete set;
}

void addVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  if (set->curSetFlags[v] == 0)
  {
    set->curSetFlags[v] = 1;
    // set->vertices[set->size] = v;
    // set->size = set->size + 1;
  }
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

  return NULL;
}

