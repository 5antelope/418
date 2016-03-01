#include "paraGraph.h"
#include "graph.h"
#include <stdio.h>
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

    Graph_decomposition(Graph g, int* solution, int* dus, int maxVal, int maxId, int* updateAtIter, int* iter)
            : g(g), solution(solution), dus(dus), maxVal(maxVal), maxId(maxId),updateAtIter(updateAtIter), iter(iter)
    {
#pragma omp parallel for
        for (int i = 0; i < num_nodes(g); i++) {
            solution[i] = -1;
            updateAtIter[i]=-1;
        }
        solution[maxId]=maxId;
    }

    bool update(Vertex src, Vertex dst) {
        //printf("update dst=%d, updateAtIter=%d, iter=%d\n",dst, updateAtIter[dst],*iter);
//        if (distances_[dst] == NA)
//            return __sync_bool_compare_and_swap(&distances_[dst], NA, currentDistance);
        updateAtIter[dst] = *iter;
        bool r;


        if (solution[dst] == -1) {
            //printf("-1 writting src %d's cluster %d to %d\n", src, solution[src], dst);
            r= __sync_bool_compare_and_swap(&solution[dst], -1, solution[src]);
        }

        if (solution[dst] > solution[src]) {
            //printf("> writting src %d's cluster %d to %d\n", src, solution[src], dst);
            r= __sync_bool_compare_and_swap(&solution[dst], solution[dst], solution[src]);
        }
        r= true;

        return r;
    }

    //bottom up only
    bool cond(Vertex src, Vertex v) {
        if(solution[v]==-1) {
            return true;
        }
        if(solution[v]!=-1&&updateAtIter[v]==*iter) {
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
   // printf("add index %d to be cluster!\n",maxId);


    Graph_decomposition f( g, decomp, dus, maxVal,  maxId, updateAtIter,  &iter);

    VertexSet *newFrontier;

    while (frontier->size > 0)
    {
       // printf("iter=%d, frontier->size=%d\n",iter,frontier->size);
        newFrontier = edgeMap<Graph_decomposition>(g, frontier, f);
        //printf("newFrontier->size=%d\n",newFrontier->size);
        //printf("iter=%d\n",f.iter);
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
                   // printf("grow index %d to be cluster! (maxVal - dus[i])=%d\n",i,(maxVal - dus[i]));
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

//        free(frontier->curSetFlags);
//        frontier=new_frontier;
    }

    free(updateAtIter);
}