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

// Epsilon for approximate float comparisons
#define EPSILON 0.001f

// Output column size
#define COL_SIZE 15

/*
 * Printing functions
 */

static void sep(std::ostream& out, char separator = '-', int length = 78)
{
    for (int i = 0; i < length; i++)
      out << separator;
    out << std::endl;
}

static void printTimingApp(std::ostream& timing, const char* appName)
{
  std::cout << std::endl;
  std::cout << "Timing results for " << appName << ":" << std::endl;
  sep(std::cout, '=', 75);

  timing << std::endl;
  timing << "Timing results for " << appName << ":" << std::endl;
  sep(timing, '=', 75);
}

/*
 * Correctness checkers
 */

template <class T>
bool compareArrays(Graph graph, T* ref, T* stu)
{
  for (int i = 0; i < graph->num_nodes; i++) {
    if (ref[i] != stu[i]) {
      std::cerr << "*** Results disagree at " << i << " expected " 
        << ref[i] << " found " << stu[i] << std::endl;
      return false;
    }
  }
  return true;
}

template <class T>
bool compareApprox(Graph graph, T* ref, T* stu)
{
  for (int i = 0; i < graph->num_nodes; i++) {
    if (abs(ref[i] - stu[i]) > EPSILON) {
      std::cerr << "*** Results disagree at " << i << " expected " 
        << ref[i] << " found " << stu[i] << std::endl;
      return false;
    }
  }
  return true;
}

template <class T>
bool compareArraysAndDisplay(Graph graph, T* ref, T*stu) 
{
  printf("\n----------------------------------\n");
  printf("Visualization of student results");
  printf("\n----------------------------------\n\n");

  int grid_dim = (int)sqrt(graph->num_nodes);
  for (int j=0; j<grid_dim; j++) {
    for (int i=0; i<grid_dim; i++) {
      printf("%02d ", stu[j*grid_dim + i]);
    }
    printf("\n");
  }
  printf("\n----------------------------------\n");
  printf("Visualization of reference results");
  printf("\n----------------------------------\n\n");

  grid_dim = (int)sqrt(graph->num_nodes);
  for (int j=0; j<grid_dim; j++) {
    for (int i=0; i<grid_dim; i++) {
      printf("%02d ", ref[j*grid_dim + i]);
    }
    printf("\n");
  }
  
  return compareArrays<T>(graph, ref, stu);
}

template <class T>
bool compareArraysAndRadiiEst(Graph graph, T* ref, T* stu) 
{
  bool isCorrect = true;
  for (int i = 0; i < graph->num_nodes; i++) {
    if (ref[i] != stu[i]) {
      std::cerr << "*** Results disagree at " << i << " expected "
        << ref[i] << " found " << stu[i] << std::endl;
	isCorrect = false;
    }
  }
  int stuMaxVal = -1;
  int refMaxVal = -1;
  #pragma omp parallel for schedule(dynamic, 512) reduction(max: stuMaxVal)
  for (int i = 0; i < graph->num_nodes; i++) {
	if (stu[i] > stuMaxVal)
		stuMaxVal = stu[i];
  }
  #pragma omp parallel for schedule(dynamic, 512) reduction(max: refMaxVal)
  for (int i = 0; i < graph->num_nodes; i++) {
        if (ref[i] > refMaxVal)
                refMaxVal = ref[i];
  }
 
  if (refMaxVal != stuMaxVal) {
	std::cerr << "*** Radius estimates differ. Expected: " << refMaxVal << " Got: " << stuMaxVal << std::endl;
	isCorrect = false;
  }   
  return isCorrect;
}

/*
 * Time and score an app
 */

// Returns score for the app.
template<typename T, int APP>
double timeApp(Graph g, int device, int numTrials, double maxPoints,
    int minThreadCount, int maxThreadCount,
    void (*ref)(Graph, T*), void (*stu)(Graph, T*),
    bool (*check)(Graph, T*, T*),
    std::ostream& timing) {

  timing << std::left << std::setw(COL_SIZE) << "Threads";
  timing << std::left << std::setw(COL_SIZE) << "Ref. Time";
  timing << std::left << std::setw(COL_SIZE) << "Ref. Speedup";
  timing << std::left << std::setw(COL_SIZE) << "Your Time";
  timing << std::left << std::setw(COL_SIZE) << "Your Speedup";
  timing << std::endl;
  sep(timing, '-', 75);

  std::cout << std::left << std::setw(COL_SIZE) << "Threads";
  std::cout << std::left << std::setw(COL_SIZE) << "Ref. Time";
  std::cout << std::left << std::setw(COL_SIZE) << "Ref. Speedup";
  std::cout << std::left << std::setw(COL_SIZE) << "Your Time";
  std::cout << std::left << std::setw(COL_SIZE) << "Your Speedup";
  std::cout << std::endl;
  sep(std::cout, '-', 75);

  using namespace std::chrono;
  typedef std::chrono::high_resolution_clock Clock;
  typedef std::chrono::duration<double> dsec;

  int precision = 4;
  T* refSolution = new T [g->num_nodes];
  T* stuSolution = new T [g->num_nodes];

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
      ref(g, refSolution);
      refTime += duration_cast<dsec>(Clock::now() - refStart).count();

// Don't run student's solution if compiled with REF_ONLY
#ifdef REF_ONLY
      stuTime += 1;
#else
      auto stuStart = Clock::now();
      stu(g, stuSolution);
      stuTime += duration_cast<dsec>(Clock::now() - stuStart).count();

      correct = correct && check(g, refSolution, stuSolution);
      if (!correct)
        break;
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
    timing << std::left << std::setw(COL_SIZE - 7) << "";
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(COL_SIZE) << refTime;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(COL_SIZE) << refSpeedup;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(COL_SIZE) << stuTime;
    timing << std::setprecision(precision) << std::fixed;
    timing << std::left << std::setw(COL_SIZE) << stuSpeedup;
    timing << std::endl;
    std::cout << std::right << std::setw(7) << threads;
    std::cout << std::left << std::setw(COL_SIZE - 7) << "";
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(COL_SIZE) << refTime;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(COL_SIZE) << refSpeedup;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(COL_SIZE) << stuTime;
    std::cout << std::setprecision(precision) << std::fixed;
    std::cout << std::left << std::setw(COL_SIZE) << stuSpeedup;
    std::cout << std::endl;

    if (threads == maxThreadCount)
      break;
    
    threads = std::min(maxThreadCount, threads * 2);
  }

  delete[] refSolution;
  delete[] stuSolution;

  /* Print and return grade */
  double fraction = stuBestTime / refBestTime * 100.0;
  double curve = 4.0 / 3.0 * (refBestTime / stuBestTime) - (1.0 / 3.0);
  double points = std::min(maxPoints, std::max(maxPoints * curve, 0.0));
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

#endif /* __GRADE_H__ */
