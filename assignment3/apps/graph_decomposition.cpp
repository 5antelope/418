#include "paraGraph.h"
#include "graph.h"
#include <stdio.h>
#include <mutex>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <semaphore.h>

class Graph_decomposition
{
public:
    Graph g;
    int* solution;
    int* dus;
    int maxVal;
    int maxId;
    int * updateAtIter;
    int * iter;
    sem_t* mtx;

    Graph_decomposition(Graph g, int* solution, int* dus, int maxVal, int maxId, int* updateAtIter, int* iter)
            : g(g), solution(solution), dus(dus), maxVal(maxVal), maxId(maxId),updateAtIter(updateAtIter), iter(iter)
    {
        mtx = (sem_t *)malloc(sizeof(sem_t) * num_nodes(g));
        #pragma omp parallel for
        for (int i = 0; i < num_nodes(g); i++) {
            solution[i] = -1;
            updateAtIter[i]=-1;
            sem_init(&mtx[i], 0, 1);
        }
        solution[maxId]=maxId;
    }

    bool update(Vertex src, Vertex dst) {
        updateAtIter[dst] = *iter;
        bool r = false;

        sem_wait(&mtx[dst]);

        if (solution[dst] == -1) {
            solution[dst] = solution[src];
            r = true;
        }
        else if (solution[dst] > solution[src]) {
            solution[dst] = solution[src];
            r = true;
        }

        sem_post(&mtx[dst]);

        return r;
    }

    //bottom up only
    bool cond(Vertex src, Vertex v) {
        if(solution[v]==-1) {
            return true;
        }
        if(solution[v]!=-1 && updateAtIter[v]==*iter) {
            return true;
        }
        return false;
    }
    //top down and bottom up
    bool cond(Vertex v) {
        return true;
    }


private:

};
/**
	Given a graph, a deltamu per node, the max deltamu value, and the id
	of the node with the max deltamu, decompose the graph into clusters.
        Returns for each vertex the cluster id that it belongs to inside decomp.
	NOTE: deltamus are given as integers, floating point differences
	are resolved by node id order

**/
void decompose(graph *g, int *decomp, int* dus, int maxVal, int maxId) {

    //initialize decomp? no need
    int * updateAtIter = (int *)malloc(num_nodes(g)* sizeof(int));


    VertexSet* frontier = newVertexSet(SPARSE, 1, num_nodes(g));
    addVertex(frontier,maxId);

    int iter=0;

    Graph_decomposition f( g, decomp, dus, maxVal,  maxId, updateAtIter,  &iter);

    VertexSet *newFrontier;

    while (frontier->size > 0)
    {
        newFrontier = edgeMap<Graph_decomposition>(g, frontier, f);
        freeVertexSet(frontier);
        frontier = newFrontier;
        iter++;

        #pragma omp parallel for schedule(guided)
        for (int i = 0; i < num_nodes(g); i++)
        {
            if (f.solution[i] == -1)
            {
                if ((maxVal - dus[i])<iter)
                {
                    newFrontier->curSetFlags[i] = 1;
                    f.solution[i]=i;
                }
            }
        }

        int sum = 0;
        #pragma omp parallel for reduction(+:sum)
        for (int i=0; i<num_nodes(g); i++)
            sum += newFrontier->curSetFlags[i];

        newFrontier->size = sum;
    }

    free(updateAtIter);
}
