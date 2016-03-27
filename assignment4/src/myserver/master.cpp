#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <map>
#include <vector>
#include <limits>

#include "server/messages.h"
#include "server/master.h"
#include "tools/cycle_timer.h"

using namespace std;

static const int THRESHOLD_HIGH = 30;
static const int THRESHOLD_LOW = 10;

Worker_handle pick_idle_woker(string cmd);

typedef struct {

  // Keep meta data about workers:
  // 0) is it avitve
  // 1) how many pendings on the worker
  // 2) how many footprint requests on the worker
  // 3) how many arithmetic requests on the worker
  // 4) latest latency from the worker

  bool is_active;

  int num_pending_requests;
  int num_footprint_requests;

} Worker_meta;

typedef struct {
  int tag;
  int count;

  int arg[4];

} Prime_info;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;

  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  int num_workers;

  // worker_list keeps track of workers
  vector<Worker_handle> worker_list;

  // client_map maps request to clients by next_tag int
  // Client_handle waiting_client;
  map<int, Client_handle> client_map;

  map<int, Request_msg> request_map;

  // map worker to corresponding meta data
  map<Worker_handle, Worker_meta> worker_map;

  // map tag to request type
  // 0: cache footprint
  // 1: othres
  map<int, int> request_type;

  // cache for prime
  map<int, int> prime_cache;
  // map tag to prime job information
  map<int, Prime_info> prime_map;

  // cache for others
  map<string, string> normal_cache;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.num_workers = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  
  mstate.server_ready = false;

  // fire off a request for a new worker
  // take normal tag number

  // TOOD: update how many worker to lauch at init
  for (int i=0; i<max_workers; i++) {
    int tag = mstate.next_tag++;
    Request_msg req(tag);

    string name = "my worker " + to_string(tag);
    req.set_arg("name", name);

    request_new_worker_node(req);
  }

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  // TODO: 
  // Worker_meta: 
  // 1) is_active = true
  // 2) num_pending_requests = 0
  // 3) num_footprint_requests = 0
  // 4) latest_latency = 0
  // ** add Worker_meta to worker_map 

  Worker_meta meta;

  meta.is_active = true;
  meta.num_pending_requests = 0;
  meta.num_footprint_requests = 0;

  mstate.worker_map[worker_handle] = meta;

  mstate.worker_list.push_back(worker_handle);

  mstate.num_workers++;
  DLOG(INFO) << "worker: " << mstate.num_workers << " on line" << std::endl;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  int tag = resp.get_tag();

  Client_handle client = mstate.client_map[tag];

  send_client_response(client, resp);

  // clear number of pening client request and
  // add a free worker to queue to use
  mstate.num_pending_client_requests--;

  Request_msg req = mstate.request_map[tag].request_msg;
  
  string value = resp.get_response();

  if (mstate.prime_map.find(tag) != mstate.prime_map.end()) {
    // request is compare prime request
    Prime_info prime_info = mstate.prime_map[tag];

    prime_info.count++;

    // put response to cache
    int n = atoi(req.get_arg("n").c_str());
    
    prime_cache[n] = atoi(value.c_str);
  }

  // decrease the worker_count in meta data
  Worker_meta meta = mstate.worker_map[worker_handle];
  if (mstate.request_type[tag] == 0) {
    // cache footprint
    meta.num_footprint_requests--;
  }
  else {
    // other request types
    meta.num_pending_requests--;
  }

  mstate.normal_cache[req.get_arg("x")] = value;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.

  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  // The provided starter code cannot handle multiple pending client
  // requests.  The server returns an error message, and the checker
  // will mark the response as "incorrect"

  // Save off the handle to the client that is expecting a response.
  // The master needs to do this it can response to this client later
  // when 'handle_worker_response' is called.

  mstate.num_pending_client_requests++;

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.

  string cmd = client_req.get_arg("cmd");

  if (cmd.compare("compareprimes") == 0) {
    // handle compareprimes by 4 separate requests
    execute_compareprimes(client_req, client_handle);

    return;
  }

  // check cache first
  string result ="";
  if (check_cache(client_req, result)) {
    DLOG(INFO) << "; Cache hit";

    Response_msg dummy_resp(0);
    dummy_resp.set_response(result);

    send_client_response(client_handle, dummy_resp);

    return;
  }
  
  // create tag-client map
  int tag = mstate.next_tag++;
  mstate.client_map.insert ( std::pair<int, Client_handle>(tag, client_handle) );
  mstate.request_map[tag] = client_req;

  // remember type of request by tags
  if (cmd.compare("projectidea") == 0)
    mstate.request_type[tag] = 0;
  else
    mstate.request_type[tag] = 1;

  // create request for worker
  Request_msg worker_req(tag, client_req);

  // select a woker for request
  Worker_handle worker = pick_idle_woker(cmd);

  send_request_to_worker(worker, worker_req);

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}

bool check_cache(Request_msg req, string &result) {

  string cmd = req.get_arg("cmd");

  if (mstate.normal_cache.find(req.get_arg("x")) != mstate.tellmenow_cache.end()) {
    result = mstate.tellmenow_cache[req.get_arg("x")];
    return true;
  }

  return false;

}

void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

void execute_compareprimes(const Request_msg& req, const Client_handle& client) {

    int params[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    Prime_info prime_info;

    int tag = mstate.next_tag++;
    prime_info.tag = tag;

    for (int i = 0; i< 4; i++) {
      Request_msg dummy_req(tag + i);
      mstate.prime_map[tag + i] = prime_info;

      create_computeprimes_req(dummy_req, params[i]);

      if (mstate.prime_cache.find(params[i]) == mstate.prime_cache.end()) {
        // not found
        Worker_handle worker = pick_idle_woker(dummy_req.get_arg("cmd"));
        send_request_to_worker(worker, dummy_req);
      }
    }

    mstate.client_map[tag] = client;

}

Worker_handle pick_idle_woker(string cmd) {

  Worker_handle worker = NULL;
  int min = numeric_limits<int>::max();

  for (vector<Worker_handle>::iterator it = mstate.worker_list.begin(); it != mstate.worker_list.end(); ++it) {

    Worker_handle tmp = *it;

    if (cmd.compare("projectidea") == 0 && mstate.worker_map[tmp].num_footprint_requests < min)
      worker = tmp;
    else if (mstate.worker_map[tmp].num_pending_requests < min)
      worker = tmp;
  
  }

  return worker;
}

void elastic_check() {

  for (vector<Worker_handle>::iterator it = mstate.worker_list.begin(); it != mstate.worker_list.end(); ++it) {

    Worker_handle worker = *it;

    int total_requests = mstate.worker_map[worker].num_pending_requests + mstate.worker_map[worker].num_footprint_requests;

    if (mstate.num_workers < mstate.max_num_workers && total_requests > THRESHOLD_HIGH) {
      // add a worker
      int tag = mstate.next_tag++;
      Request_msg req(tag);

      string name = "my worker " + to_string(tag);
      req.set_arg("name", name);

      request_new_worker_node(req);

      break;
    }

    if (mstate.num_workers > 1 && total_requests < THRESHOLD_LOW) {
      // invalid this worker
      mstate.worker_map[worker].is_active = false;
      mstate.num_workers--;

      break;
    }
  
  }
}

void check_and_kill() {

  // check all worker_list and their meta data, send kill request to
  // woker which is in-active and pending job number = 0
  for (vector<Worker_handle>::iterator it = mstate.worker_list.begin(); it != mstate.worker_list.end(); ++it) {

     if (!mstate.worker_map[*it].is_active
      && mstate.worker_map[*it].num_pending_requests == 0 && mstate.worker_map[*it].num_footprint_requests == 0 ) {

      kill_worker_node(*it);
    }

  }

}

void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

  // TODO: check workers' queue counts
  elastic_check();

  check_and_kill();

}

