#ifndef __GRADE_H__
#define __GRADE_H__

#include <stdio.h>
#include <sstream>
#include <iomanip>
#include <chrono>

#include <type_traits>
#include <utility>

#include <float.h>

#include <omp.h>
#include "mic.h"

#include "graph.h"
#include "graph_internal.h"

#define MAX_POINTS 4.5


void visualize_results_grid(int* results, int dim);

static void sep(std::ostream& out, char separator = '-', int length = 78)
{
    for (int i = 0; i < length; i++)
      out << separator;
    out << std::endl;
}

/* Phi wrapper */
template<typename Fn, Fn fn, typename T, typename... Args>
typename std::result_of<Fn(Graph, T*, Args...)>::type
mic_wrapper( int num_nodes, int *outgoing_starts
           , int *outgoing_edges, int *incoming_starts
           , int *incoming_edges, int num_edges
           , T* solution
           , Args... args
           )
{
  graph g;

  g.num_nodes = num_nodes;
  g.num_edges = num_edges;
  g.outgoing_starts = outgoing_starts;
  g.outgoing_edges = outgoing_edges;
  g.incoming_starts = incoming_starts;
  g.incoming_edges = incoming_edges;

  return fn (&g, solution, args...);
}
#define MIC_WRAPPER(FUNC, T) mic_wrapper<decltype(&(FUNC)), &(FUNC), T>


// template<typename Fn, Fn ref, Fn stu, typename T, typename... Args>
template<typename Fn, Fn ref, Fn stu, typename T>
double timeMic
  ( std::stringstream& timing
  , int device
  , int numTrials
  , int minThreadCount
  , int maxThreadCount 
  , bool (*check)(Graph g, T* refSolution, T* stuSolution)
  , Graph graph
  // , Args... args
  )
{
  // typedef typename std::result_of<Fn(Graph, Args...)>::type result_type;
  using namespace std::chrono;
  typedef std::chrono::high_resolution_clock Clock;
  typedef std::chrono::duration<double> dsec;

  int colSize = 15;
  int precision = 4;

  /* Transfer graph data to device */
  #ifdef RUN_MIC
  int num_edges = graph->num_edges;
  int num_nodes = graph->num_nodes;

  int* outgoing_starts = graph->outgoing_starts;
  int* outgoing_edges = graph->outgoing_edges;

  int* incoming_starts = graph->incoming_starts;
  int* incoming_edges = graph->incoming_edges;
  #endif

  #pragma offload_transfer target(mic: device) \
        in(outgoing_starts : length(graph->num_nodes) ALLOC) \
        in(outgoing_edges : length(graph->num_edges) ALLOC)  \
        in(incoming_starts : length(graph->num_nodes) ALLOC) \
        in(incoming_edges : length(graph->num_edges) ALLOC)


  /* Run tests */
  timing << std::left << std::setw(colSize) << "Threads";
  timing << std::left << std::setw(colSize) << "Ref. Time";
  timing << std::left << std::setw(colSize) << "Ref. Speedup";
  timing << std::left << std::setw(colSize) << "Your Time";
  timing << std::left << std::setw(colSize) << "Your Speedup";
  timing << std::endl;
  sep(timing, '-', 75);

  std::cout << std::left << std::setw(colSize) << "Threads";
  std::cout << std::left << std::setw(colSize) << "Ref. Time";
  std::cout << std::left << std::setw(colSize) << "Ref. Speedup";
  std::cout << std::left << std::setw(colSize) << "Your Time";
  std::cout << std::left << std::setw(colSize) << "Your Speedup";
  std::cout << std::endl;
  sep(std::cout, '-', 75);


  T* refSolution = new T [graph->num_nodes];
  T* stuSolution = new T [graph->num_nodes];

  bool firstTimeDone = false;
  double refOneThreadTime = 0;
  double stuOneThreadTime = 0;

  bool correct = true;
  double refBestTime = DBL_MAX;
  double stuBestTime = DBL_MAX;

  int threads = minThreadCount;
  while (true) {
    double refTime = 0;
    double stuTime = 0;
    for (int i = 0; i < numTrials; i++) {

      #pragma offload target(mic: device)
      omp_set_num_threads(threads);

      auto refStart = Clock::now();
      #pragma offload target(mic: device) \
        in(num_edges) \
        in(num_nodes) \
        nocopy(outgoing_starts : length(graph->num_nodes) REUSE) \
        nocopy(outgoing_edges : length(graph->num_edges) REUSE)  \
        nocopy(incoming_starts : length(graph->num_nodes) REUSE) \
        nocopy(incoming_edges : length(graph->num_edges) REUSE)  \
        out(refSolution : length(graph->num_nodes))
      #ifdef RUN_MIC
      ref
        ( num_nodes, outgoing_starts, outgoing_edges
        , incoming_starts, incoming_edges, num_edges
        , refSolution
        // , args...
        );
      #else
      ref(graph, refSolution);
      #endif
      refTime += duration_cast<dsec>(Clock::now() - refStart).count();

// Don't run student's solution if compiled with REF_ONLY
#ifdef REF_ONLY
      stuTime += 1;
#else
      auto stuStart = Clock::now();
      #pragma offload target(mic: device) \
        in(num_edges) \
        in(num_nodes) \
        nocopy(outgoing_starts : length(graph->num_nodes) REUSE) \
        nocopy(outgoing_edges : length(graph->num_edges) REUSE)  \
        nocopy(incoming_starts : length(graph->num_nodes) REUSE) \
        nocopy(incoming_edges : length(graph->num_edges) REUSE)  \
        out(stuSolution : length(graph->num_nodes))
      #ifdef RUN_MIC
      stu
        (num_nodes, outgoing_starts, outgoing_edges
        , incoming_starts, incoming_edges, num_edges
        , stuSolution
        // , args...
        );
      #else
      stu(graph, stuSolution);
      #endif
      stuTime += duration_cast<dsec>(Clock::now() - stuStart).count();

      correct = correct && check(graph, refSolution, stuSolution);
#endif /* REF_ONLY */
    }

    refTime /= numTrials;
    stuTime /= numTrials;

    if (!firstTimeDone) {
      firstTimeDone = true;
      refOneThreadTime = refTime;
      stuOneThreadTime = stuTime;
    }

    refBestTime = std::min(refBestTime, refTime);
    stuBestTime = std::min(stuBestTime, stuTime);

    double refSpeedup = refOneThreadTime / refTime;
    double stuSpeedup = stuOneThreadTime / stuTime;

    timing << std::right << std::setw(7) << threads;
    timing << std::left << std::setw(colSize - 7) << "";
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(colSize) << refTime;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(colSize) << refSpeedup;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(colSize) << stuTime;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(colSize) << stuSpeedup;
    timing << std::endl;
    std::cout << std::right << std::setw(7) << threads;
    std::cout << std::left << std::setw(colSize - 7) << "";
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(colSize) << refTime;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(colSize) << refSpeedup;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(colSize) << stuTime;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(colSize) << stuSpeedup;
    std::cout << std::endl;

    if (threads == maxThreadCount)
      break;
    
    threads = std::min(maxThreadCount, threads * 2);
  }

  /* Free graph data */
  #pragma offload_transfer target(mic: device) \
        nocopy(outgoing_starts : length(graph->num_nodes) FREE) \
        nocopy(outgoing_edges : length(graph->num_edges) FREE)  \
        nocopy(incoming_starts : length(graph->num_nodes) FREE) \
        nocopy(incoming_edges : length(graph->num_edges) FREE)

  delete[] refSolution;
  delete[] stuSolution;

  /* Print and return grade */
  double fraction = stuBestTime / refBestTime * 100.0;
  double curve = 4.0 / 3.0 * (refBestTime / stuBestTime) - (1.0 / 3.0);
  double points = std::min(MAX_POINTS, std::max(MAX_POINTS * curve, 0.0));
  points = (correct) ? points : 0.0;

  sep(timing, '-', 75);
  timing << "Time: " << std::setprecision(2) << std::fixed << fraction << '%' << " of reference solution.";
  if (correct) {
    timing << " Grade: " << std::fixed << std::setprecision(2) << points << std::endl;
  } else
  {
    timing << " Grade: " << std::fixed << std::setprecision(2) << "INCORRECT" << std::endl;
  }
  sep(timing, '-', 75);

  sep(std::cout, '-', 75);
  std::cout << "Time: " << std::setprecision(2) << std::fixed << fraction << '%' << " of reference solution.";
  if (correct) {
      std::cout << " Grade: " << std::fixed << std::setprecision(2) << points << std::endl;
  } else
  {
      std::cout << " Grade: " << std::fixed << std::setprecision(2) << "INCORRECT" << std::endl;
  }
  sep(std::cout, '-', 75);

  return points;
}

#ifdef RUN_MIC
  #define TIME_MIC(REF, STU, T) \
    timeMic<decltype(&(MIC_WRAPPER(REF, T))), &(MIC_WRAPPER(REF, T)), &(MIC_WRAPPER(STU, T)), T>
#else
  #define TIME_MIC(REF, STU, T) timeMic<decltype(&REF), &REF, &STU, T>
#endif

#endif
