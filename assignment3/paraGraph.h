#ifndef __PARAGRAPH_H__
#define __PARAGRAPH_H__

#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unordered_set>

#include <omp.h>

#include "vertex_set.h"
#include "graph.h"

#include "mic.h"

#define NUM_THREADS 2

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
  // omp_set_num_threads(NUM_THREADS);
  // omp_set_schedule(omp_sched_dynamic, 10)

  // #pragma omp parallel for reduction(+:k)

  VertexSet* set = newVertexSet(SPARSE, g->num_nodes, g->num_nodes);
  memcpy(set->flags, u->flags, sizeof(int) * g->num_nodes);

  for (int i=0; i<g->num_nodes; i++)
  {
    const Vertex* start = incoming_begin(g, (Vertex)i);
    const Vertex* end = incoming_end(g, (Vertex)i);

    for (const Vertex* v=start; v!=end; v++)
    {
      // TODO: HARD CODE src of update() here
      if (u->flags[*v] == 1 && f.update(0, i))
      {
        addVertex(set, i);
        set->flags[i] = 1;
        break;
      }
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
