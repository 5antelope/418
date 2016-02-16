#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <omp.h>
#include <bitset>
#include <random>
#include <math.h>
#include <mutex> 
#include <limits.h>

#include "paraGraph.h"
#include "mic.h"
#include "graph.h"
#include "graph_internal.h"

#define NONE INT_MAX
#define YESRETURN 1
#define NORETURN 0

/**
	Given a graph, and a deltamu per node, the index with the 
	max delta mu decomposes it into clusters. Returns for each 
	vertex the cluster id that it belongs to.
	NOTE: deltamus are given as integers, floating point differences
	are resolved by node id order
**/
void decompose(graph *g, int *decomp, float beta) {
}
