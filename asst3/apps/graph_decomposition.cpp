#include "paraGraph.h"
#include "graph.h"
#include <stdio.h>

//#define DEBUG
#ifdef DEBUG
# define dbg_printf(...) printf(__VA_ARGS__)
#else
# define dbg_printf(...)
#endif

void printGraph(graph* graph)
{
    printf("==================================================================\n");

    printf("Graph pretty print:\n");
    printf("num_nodes=%d\n", graph->num_nodes);
    printf("num_edges=%d\n", graph->num_edges);

    for (int i=0; i<graph->num_nodes; i++) {

        int start_edge = graph->outgoing_starts[i];
        int end_edge = (i == graph->num_nodes-1) ? graph->num_edges : graph->outgoing_starts[i+1];
        printf("node %02d: out=%d: ", i, end_edge - start_edge);
        for (int j=start_edge; j<end_edge; j++) {
            int target = graph->outgoing_edges[j];
            printf("%d ", target);
        }
        printf("\n");

        start_edge = graph->incoming_starts[i];
        end_edge = (i == graph->num_nodes-1) ? graph->num_edges : graph->incoming_starts[i+1];
        printf("           in=%d: ", end_edge - start_edge);
        for (int j=start_edge; j<end_edge; j++) {
            int target = graph->incoming_edges[j];
            printf("%d ", target);
        }
        printf("\n");
    }
    printf("==================================================================\n");
}
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
    #pragma omp parallel for schedule(dynamic)
    for(int i = 0; i< num_nodes(g); i++){
        decomp[i]=-1;
        updateAtIter[i]=-1;
    }

    dbg_printf("decompose maxId=%d\n",maxId);

    VertexSet* frontier = newVertexSet(SPARSE, 1, num_nodes(g));
    addVertex(frontier,maxId);
    frontier->size=1;
    decomp[maxId]=maxId;
    updateAtIter[maxId]=0;

    int iter = 0;

    dbg_printf("frontier->size, %d\n",frontier->size);
    //printGraph(g);
    dbg_printf("decomp 0=%d\n",decomp[15]);

    while (frontier->size > 0) {
        //print status of each iteration
        VertexSet* new_frontier = newVertexSet(SPARSE, 1, num_nodes(g));
        dbg_printf("status: ");
            int count = 0;
            for (int i = 0; i < frontier->numNodes; i++) {
                if (frontier->curSetFlags[i] == 1)
                    count++;
                dbg_printf("%d ", frontier->curSetFlags[i]);
            }
        dbg_printf("size: %d", frontier->size);
        dbg_printf("\n");

        //for each vertex in frontier
#pragma omp parallel for schedule(static)
        for (int i = 0; i < frontier->numNodes; i++) {
            if (frontier->curSetFlags[i] == 1) {
                int u = i;
                //incoming edges/ outgoing edges...
                const Vertex *start = outgoing_begin(g, u);
                const Vertex *end = outgoing_end(g, u);
                //for each outgoing edge
//#pragma omp parallel for
                for (const Vertex *v = start; v != end; v++) {
//#pragma omp single
  //                  {
                        if (decomp[*v] == -1) {
                            //mark them as cluster id
                            dbg_printf("mark %d vertex to be u=%d,cluster[u]=%d \n", *v, u, decomp[u]);
                            decomp[*v] = decomp[u];
                            updateAtIter[*v] = iter;
                            new_frontier->curSetFlags[*v] = 1;

                            //new_frontier->size++;
                        } else if (updateAtIter[*v] == iter) {
                            if (decomp[u] < decomp[*v]) {
                                decomp[*v] = decomp[u];
                            }
                        }
               //     }
                }
            }
        }
        #pragma omp atomic
        iter++;

        dbg_printf("iter=%d\n", iter);
        // start growing all balls i at the next iter with
        // unvisited center i and with maxDu - dus[i] < iter
#pragma omp parallel for schedule(static)
        for (int i = 0; i < num_nodes(g); i++) {
            if (decomp[i] == -1) {
                if ((maxVal - dus[i])<iter) {
                    dbg_printf("grow %d vertex to be cluster center\n", i);
                    new_frontier->curSetFlags[i] = 1;
                    decomp[i]=i;
                    //new_frontier->size++;
                }
            }
        }

        //calculate size
        int sum = 0;
#pragma omp parallel for reduction(+:sum)
        for (int i=0; i<num_nodes(g); i++)
            sum += new_frontier->curSetFlags[i];


        new_frontier->size = sum;

        free(frontier->curSetFlags);
        frontier=new_frontier;
    }


    //print result
//    printf("result: \n");
//    for (int i=0; i<g->num_nodes; i++) {
//        printf("%d: %d \n",i,decomp[i]);
//    }
//    printf(";\n");

    free(updateAtIter);

}
