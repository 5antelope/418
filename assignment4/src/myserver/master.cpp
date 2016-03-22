#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <map>
#include <vector>

#include "server/messages.h"
#include "server/master.h"

using namespace std;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  int free_worker;
  int num_workers;

  // worker_list keeps track of workers
  // Worker_handle my_worker;
  vector<Worker_handle> worker_list;

  // client_map maps request to clients by next_tag int
  // Client_handle waiting_client;
  map<int, Client_handle> client_map;

  // request_queue buffer requests exceed number of workers
  queue<Request_msg> request_queue;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.free_worker = 0;
  mstate.num_workers = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  // take normal tag number
  for (int i=0; i<max_workers; i++) {
    int tag = mstate.next_tag++;
    Request_msg req(tag);
    // req.set_arg("name", "my worker 0");
    req.set_arg("name", "my worker "+tag);

    request_new_worker_node(req);
  }

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  mstate.worker_list.push_back(worker_handle);
  mstate.free_worker++;
  mstate.num_workers++;
  DLOG(INFO) << "worker: " << mstate.num_workers << std::endl;

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
  map<int, Worker_handle>::iterator itr = mstate.client_map.find(tag);
  if (itr != mstate.client_map.end()) {
    // find which client needs response
    Client_handle client = itr->second;
    send_client_response(client, resp);

    // clear number of pening client request and
    // add a free worker to queue to use
    mstate.num_pending_client_requests--;

  }
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

  // mstate.waiting_client = client_handle;
  mstate.num_pending_client_requests++;

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.

  // create tag-client map
  int tag = mstate.next_tag++;
  mstate.client_map.insert ( std::pair<int, Client_handle>(tag, client_handle) );

  // create request for worker
  Request_msg worker_req(tag, client_req);

  if (mstate.num_pending_client_requests > mstate.max_num_workers) {
    mstate.request_queue.push(worker_req);
    return;
  }

  mstate.request_queue.push(worker_req);

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}

void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

  if (mstate.request_queue.size() == 0) {
    DLOG(INFO) << "Request queue is empty " << std::endl;
    return;
  }

  // pick a reqeust and a worker
  Request_msg worker_req = mstate.request_queue.front();

  int random = rand() % mstate.num_workers;
  Worker_handle worker = mstate.worker_list.at(random);

  send_request_to_worker(worker, worker_req);

  mstate.request_queue.pop();

}

