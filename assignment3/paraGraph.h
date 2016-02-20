#ifndef __PARAGRAPH_H__
#define __PARAGRAPH_H__

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "vertex_set.h"
#include "graph.h"

#include "mic.h"

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
  int k = 0;

  // #pragma omp parallel for
  for (int i=0; i<u->size; i++)
  {
    Vertex vertex = u->vertices[i];
    const Vertex* start = outgoing_begin(g, vertex);
    const Vertex* end = outgoing_end(g, vertex);
    for (const Vertex* v=start; v!=end; v++)
    {
      if (f.cond(*v))
        k++;
    }
  }

  VertexSet* set = newVertexSet(SPARSE, k, g->num_nodes);

  for (int i=0; i<u->size; i++)
  {
    Vertex vertex = u->vertices[i];
    const Vertex* start = outgoing_begin(g, vertex);
    const Vertex* end = outgoing_end(g, vertex);

    for (const Vertex* v=start; v!=end; v++)
    {
      if (f.update(vertex, *v))
        addVertex(set, *v);
    }
  }

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
  int k = 0;
  // #pragma omp parallel for
  for (int i=0; i<u->size; i++)
  {
    if (f(u->vertices[i]))
      k++;
  }

  VertexSet* set = NULL;

  if (returnSet)
  {
      set = newVertexSet(SPARSE, k, u->numNodes);
      for (int i=0; i<u->size; i++)
      {
        if (f(u->vertices[i]))
            addVertex(set, u->vertices[i]);
      }
      assert(set != NULL);
  }
  else
  {
      int i=0;
      while (i < u->size)
      {
        if (!f(u->vertices[i]))
        {
            removeVertex(u, u->vertices[i]);
            k--;
        }
        else
            i++;
      }
      assert(set == NULL);
  }

  return set;
}

#endif /* __PARAGRAPH_H__ */
