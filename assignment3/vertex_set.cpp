#include "vertex_set.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <stdio.h>
#include "mic.h"

#include <omp.h>
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

  vertexSet->curSetFlags = (int *)calloc(numNodes, sizeof(int));

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
    set->size = set->size + 1;
  }
}

void removeVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  // int i = 0;
  // for (; i < set->size; i++)
  // {
  //   if (set->vertices[i]==v)
  //       break;
  // }

  // for (; i < set->size-1; i++)
  //     set->vertices[i] = set->vertices[i+1];

  set->curSetFlags[v] = 0;
  set->size = set->size-1;
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

