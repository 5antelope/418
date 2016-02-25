#ifndef __PARAGRAPH_H__
#define __PARAGRAPH_H__

#include <iostream>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "vertex_set.h"
#include "graph.h"

#include "mic.h"

// #define DEBUG
/*
 * edgeMap --
 *
 * Students will implement this function.
 *
 * The input argument f is a class with the following methods defined:
 *   bool update(Vertex src, Vertex dst)
 *   bool cond(Vertex v)
 *
 * See apps/bfs.cpp for an example of such a class definition.
 *
 * When the argument removeDuplicates is false, the implementation of
 * edgeMap need not remove duplicate vertices from the VertexSet it
 * creates when iterating over edges.  This is a performance
 * optimization when the application knows (and can tell ParaGraph)
 * that f.update() guarantees that duplicate vertices cannot appear in
 * the output vertex set.
 *
 * Further notes: the implementation of edgeMap is templated on the
 * type of this object, which allows for higher performance code
 * generation as these methods will be inlined.
 */
template <class F>
VertexSet *edgeMap(Graph g, VertexSet *u, F &f, bool removeDuplicates=true)
{
  VertexSetType type = setType(g, u);

  VertexSet* set = newVertexSet(type, g->num_nodes, g->num_nodes);

  if (type == DENSE)
  {
    #pragma omp parallel for schedule(static)
    for (int vertex=0; vertex<g->num_nodes; vertex++)
    {
      const Vertex* start = incoming_begin(g, vertex);
      const Vertex* end = incoming_end(g, vertex);

      int len = end-start;
      if (len <10000)
      {
        for (const Vertex* v=start; v!=end; v++)
        {
          if (u->curSetFlags[*v]==1 && f.update(*v, vertex))
            addVertex(set, vertex);
        }
      }
      else
      {
        #pragma omp parallel for
        for (int i=0; i<len; i++)
        {
          const Vertex* v = start + i;
          if (u->curSetFlags[*v]==1 && f.update(*v, vertex))
            addVertex(set, vertex);
        }
      }
    }
  }
  else // SPARSE
  {
    int idx = 0;
    for (int i=0; i<g->num_nodes; i++)
    {
      if(u->curSetFlags[i]==1)
        u->vertices[idx++] = i;
    }

    #pragma omp parallel for
    for (int i=0; i<u->size; i++)
    {
      Vertex vertex = u->vertices[i];
      const Vertex* start = outgoing_begin(g, vertex);
      const Vertex* end = outgoing_end(g, vertex);

      int len = end-start;
      if (len <10000)
      {
        for (const Vertex* v=start; v!=end; v++)
        {
          if (f.update(vertex, *v))
            addVertex(set, *v);
        }
      }
      else
      {
        #pragma omp parallel for
        for (int i=0; i<len; i++)
        {
          const Vertex* v = start + i;
          if (f.update(vertex, *v))
            addVertex(set, *v);
        }
      }
    }
  }

  int sum = 0;
  #pragma omp parallel for reduction(+:sum)
  for (int i=0; i<g->num_nodes; i++)
    sum += set->curSetFlags[i];

  set->size = sum;

  return set;
}



/*
 * vertexMap --
 *
 * Students will implement this function.
 *
 * The input argument f is a class with the following methods defined:
 *   bool operator()(Vertex v)
 *
 * See apps/kBFS.cpp for an example implementation.
 *
 * Note that you'll call the function on a vertex as follows:
 *    Vertex v;
 *    bool result = f(v)
 *
 * If returnSet is false, then the implementation of vertexMap should
 * return NULL (it need not build and create a vertex set)
 */
template <class F>
VertexSet *vertexMap(VertexSet *u, F &f, bool returnSet=true)
{
  // TODO: Implement
  VertexSet* set = NULL;

  if (returnSet)
  {
      set = newVertexSet(u->type, u->numNodes, u->numNodes);
      #pragma omp parallel for schedule(static)
      for (int i=0; i<u->numNodes; i++)
      {
        if (u->curSetFlags[i]==1 && f(i))
            addVertex(set, i);
      }

      int sum = 0;
      #pragma omp parallel for reduction(+:sum)
      for (int i=0; i<u->numNodes; i++)
        sum += set->curSetFlags[i];

      set->size = sum;
  }
  else
  {
      #pragma omp parallel for schedule(static)
      for (int i=0; i<u->numNodes; i++)
      {
        if (u->curSetFlags[i]==1 && !f(i))
            removeVertex(u, i);
      }

      int sum = 0;
      #pragma omp parallel for reduction(+:sum)
      for (int i=0; i<u->numNodes; i++)
        sum += u->curSetFlags[i];

      u->size = sum;
  }

  return set;
}

#endif /* __PARAGRAPH_H__ */
