#include "vertex_set.h"

#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <stdio.h>
#include "mic.h"

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
  VertexSet vertexSet;
  vertexSet.size = capacity;
  vertexSet.numNodes = numNodes;
  vertexSet.type = type;

  // TODO: is array the best choice here?
  // not every efficient in add/delete operation
  Vertex array[capacity] = {-1};
  vertexSet.vertices = array;// (Vertex*)malloc(sizeof(Vertex) * capacity);

  return vertexSet;
}

void freeVertexSet(VertexSet *set)
{
  // TODO: Implement
  free((Vertex*)(set->vertices));
  delete set;
}

void addVertex(VertexSet *set, Vertex v)
// void addVertex(VertexSet *set, Vertex v, int k)
{
  // TODO: Implement
  int i = 0;
  for (; i<set.size; i++)
  {
  	if (set.vertices[i]==-1)
  		break;
  }
  set.vertices[i] = v;
}

void removeVertex(VertexSet *set, Vertex v)
{
  // TODO: Implement
  for (int i=0; i<set.size; i++)
  {
  	if (set.vertices[i]==v)
  	{
  		set.vertices[i] = -1;
  		break;
  	}

  }
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

