#ifndef __PARAGRAPH_H__
#define __PARAGRAPH_H__

#include <iostream>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "vertex_set.h"
#include "graph.h"
#include <omp.h>
#include "mic.h"

#define THRESHOLD 10000

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
    //frontier
    VertexSet* set = newVertexSet(type, g->num_nodes, g->num_nodes);


    if(type==DENSE){
        //bottom up
        //printf("bottom up\n");
    #pragma omp parallel for schedule(guided)
        //loop through all vertexes in Graph, if vertexes' incoming edge is in u, add itself into frontier...
        for (int vertex=0; vertex<g->num_nodes; vertex++){
            //if v has been visited, continue

            if(!f.cond(vertex)){
                //printf("continue1 %d, reject? %d\n",vertex,!f.cond(vertex));
                continue;
            }
            if(set->curSetFlags[vertex])  //||u->curSetFlags[vertex] TODO why this is not working?
                continue;

            const Vertex* start = incoming_begin(g, vertex);
            const Vertex* end = incoming_end(g, vertex);

            for (const Vertex* v=start; v!=end; v++)
            {
                if(!f.cond(*v, vertex)){
                    // printf("continue2 %d\n",vertex);
                    continue;
                }
                if (u->curSetFlags[*v] && f.update(*v, vertex)) {
                    //printf("added %d\n",vertex);
                    set->curSetFlags[vertex] = true;
                    //addVertex(set, vertex);
                }
            }


        }
    }
    else if(type== PAGERANK){
        //bottom up
    #pragma omp parallel for schedule(guided)
        for (int vertex=0; vertex<g->num_nodes; vertex++){
            if(u->curSetFlags[vertex]==true)
                continue;
            const Vertex* start = incoming_begin(g, vertex);
            const Vertex* end = incoming_end(g, vertex);

            for (const Vertex* v=start; v!=end; v++)
            {
                if (u->curSetFlags[*v] && f.update(*v, vertex))
                    set->curSetFlags[vertex] = true;
                //addVertex(set, vertex);
            }

        }
    }else if (type==SPARSE){
        //top down
        //printf("top down\n");

        #pragma omp parallel for schedule(guided)
        for(int i=0; i<g->num_nodes; i++){
            if(!u->curSetFlags[i])
                continue;
            //if(set->curSetFlags[i])  //TODO
              //  continue;
            const Vertex *start = outgoing_begin(g, i);
            const Vertex *end = outgoing_end(g, i);

            for (const Vertex *v = start; v != end; v++) {
//                if(u->curSetFlags[*v]){
//                    continue;
//                }

                if (f.cond(i,*v) && f.update(i, *v)) {
                    set->curSetFlags[*v] = true;
                    //set->visited[*v]=true;
                    //addVertex(set, *v);
                }
            }


        }
    }

    int sum = 0;
#pragma omp parallel for schedule(static) reduction(+:sum)
    for (int i=0; i<g->num_nodes; i++)
    {
        sum += set->curSetFlags[i];
    }

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
#pragma omp parallel for schedule(guided)
        for (int i=0; i<u->numNodes; i++)
        {
            if (u->curSetFlags[i]==1 && f(i)){
                set->curSetFlags[i] = true;
                //addVertex(set, i);
            }
        }

        int sum = 0;
#pragma omp parallel for schedule(static) reduction(+:sum)
        for (int i=0; i<u->numNodes; i++)
        {
            sum += set->curSetFlags[i];
        }

        set->size = sum;
    }
    else
    {
#pragma omp parallel for schedule(guided)
        for (int i=0; i<u->numNodes; i++)
        {
            if (u->curSetFlags[i] && !f(i)){
                u->curSetFlags[i] = false;
                //removeVertex(u, i);
            }
        }

        int sum = 0;
#pragma omp parallel for schedule(static) reduction(+:sum)
        for (int i=0; i<u->numNodes; i++)
        {
            sum += u->curSetFlags[i];
        }

        u->size = sum;

    }

    return set;
}

#endif /* __PARAGRAPH_H__ */


