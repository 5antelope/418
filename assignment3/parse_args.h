#ifndef __PARSE_ARGS
#define __PARSE_ARGS

typedef enum {
  BFS,
  PAGERANK,
  KBFS,
  DECOMP,
  GRADE
} App;

typedef struct
{
  App app;
  const char* graph;
  int device;
  int threads;
  bool correctness;
} Arguments;

Arguments parseArgs(int argc, char** argv);

#endif
