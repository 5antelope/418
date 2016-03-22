#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <queue>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"

#define NUM_THREADS = 24

#define DEBUG

// request_queue: queue all requests sent to this worker
static queue<Request_msg> request_queue;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";
  
  pthread_t poll[NUM_THREADS];

  for (int i=0; i<NUM_THREADS; i++) {
    pthread_create(&poll[i], NULL, routine, NULL);
    pthread_detach(poll[i]);
  }
}

void routine() {

  // The routine of worker thread: take a request from queue and 
  // process. When the process finishes, remove the request from queue.
  while(1) {
    if (!request_queue.empty()) {
      Request_msg reqeust = request_queue.front();
      worker_process_request(*request);
      request_queue.pop();
    }
  }
}

void worker_handle_request(const Request_msg& req) {

  // put all requests to a queue, wait worker threads to process
  request_queue.push(*req);

}

void worker_process_request(const Request_msg& req) {

  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.
  Response_msg resp(req.get_tag());

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  #ifdef DEBUG
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  double startTime = CycleTimer::currentSeconds();
  #endif

  if (req.get_arg("cmd").compare("compareprimes") == 0) {

    // The compareprimes command needs to be special cased since it is
    // built on four calls to execute_execute work.  All other
    // requests from the client are one-to-one with calls to
    // execute_work.

    execute_compareprimes(req, resp);

  } else {

    // actually perform the work.  The response string is filled in by
    // 'execute_work'
    execute_work(req, resp);

  }

  #ifdef DEBUG
  double dt = CycleTimer::currentSeconds() - startTime;
  DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";
  #endif

  // send a response string to the master
  worker_send_response(resp);
}
