#ifndef __PARAGRAPH_H__
#define __PARAGRAPH_H__

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
  // TODO: Implement
  // edgeMap(G: graph,
  //       U: vertexSubset,
  //       C: vertex -> bool,
  //       F: (vertex, vertex) -> bool) : vertexSubset
  // 
  // 1. Apply F to all edges (u,v) leaving the vertices u in U that satisfy the condition C(v) 
  // 2. return a new vertex subset containing all vertices v
  //    for which F(u,v) == true 
  int k = 0;

  // #pragma omp parallel for
  for (int i=0; i<u.size; i++)
  {
    Vertex vertex = u.vertices[i];
    const Vertex* start = outgoing_begin(g, vertex);
    const Vertex* end = outgoing_end(g, vertex);
    for (const Vertex* v=start; v!=end; v++)
    {
      if (f.cond(v))
      {
        k++;
      }
    }
  }

  VertexSet* set = newVertexSet(SPARSE, k, g.num_nodes);

  // #pragma omp parallel for
  for (int i=0; i<u.size; i++)
  {
    Vertex vertex = u.vertices[i];
    const Vertex* start = outgoing_begin(g, vertex);
    const Vertex* end = outgoing_end(g, vertex);
    for (const Vertex* v=start; v!=end; v++)
    {
      if (f.cond(v))
      {
        set.addVertex(v);
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
  if (!returnSet)
    return NULL;
  
  int k = 0;
  #pragma omp parallel for
  for (int i=0; i<u.size; i++)
  {
    if (f(u.vertices[i]))
      k++;
  }

  VertexSet* set = newVertexSet(SPARSE, k, u.numNodes);

  int index = 0;

  #pragma omp parallel for
  for (int i=0; i<u.size; i++)
  {
    if (f(u.vertices[i])) 
    {
      #pragma omp critical 
      {
        // set.vertices[index++] = u.vertices[i];
        set.addVertex(set, u.vertices[i]);
      }
    }
  }  

  return set;
}

#endif /* __PARAGRAPH_H__ */
