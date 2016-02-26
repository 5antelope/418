#include "paraGraph.h"
#include "graph.h"
#include <stdio.h>

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
    for(int i = 0; i< num_nodes(g); i++)
    {
        decomp[i]=-1;
        updateAtIter[i]=-1;
    }

    VertexSet* frontier = newVertexSet(SPARSE, 1, num_nodes(g));
    addVertex(frontier,maxId);
    frontier->size=1;
    decomp[maxId]=maxId;
    updateAtIter[maxId]=0;

    int iter = 0;

    while (frontier->size > 0)
    {
      VertexSet* new_frontier = newVertexSet(SPARSE, 1, num_nodes(g));
      int count = 0;
      for (int i = 0; i < frontier->numNodes; i++)
      {
          if (frontier->curSetFlags[i] == 1)
              count++;
      }

      #pragma omp parallel for schedule(static)
      for (int i = 0; i < frontier->numNodes; i++)
      {
        if (frontier->curSetFlags[i] == 1)
        {
          int u = i;
          //incoming edges/ outgoing edges...
          const Vertex *start = outgoing_begin(g, u);
          const Vertex *end = outgoing_end(g, u);
          //for each outgoing edge
          for (const Vertex *v = start; v != end; v++)
          {
            if (decomp[*v] == -1)
            {
                //mark them as cluster id
                decomp[*v] = decomp[u];
                updateAtIter[*v] = iter;
                new_frontier->curSetFlags[*v] = 1;
            }
            else if (updateAtIter[*v] == iter)
            {
                if (decomp[u] < decomp[*v])
                    decomp[*v] = decomp[u];
            }
          }
        }
      }

      #pragma omp atomic
      iter++;

      #pragma omp parallel for schedule(static)
      for (int i = 0; i < num_nodes(g); i++)
      {
        if (decomp[i] == -1)
        {
          if ((maxVal - dus[i])<iter)
          {
            new_frontier->curSetFlags[i] = 1;
            decomp[i]=i;
          }
        }
      }

      int sum = 0;
      #pragma omp parallel for reduction(+:sum)
      for (int i=0; i<num_nodes(g); i++)
        sum += new_frontier->curSetFlags[i];

      new_frontier->size = sum;

      free(frontier->curSetFlags);
      frontier=new_frontier;
    }

    free(updateAtIter);
}
