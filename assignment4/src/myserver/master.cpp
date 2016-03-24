#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>

#include "server/messages.h"
#include "server/master.h"

using namespace std;

struct Client_request{
  Request_msg request_msg;
  Client_handle waiting_client;
  int tag;
};


static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  //int num_pending_client_requests;
  int next_tag;

  //defines a worker to handle the work
  //only one worker so far
  //Worker_handle my_worker;
  int num_wokers;
  map<int,Worker_handle> worker_map;

  //a queue to store the requests
  map<int,Client_request> client_request_map;

} mstate;


void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_wokers = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  int tag = 1;
  Request_msg req(tag);
  req.set_arg("name", "my worker 1");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  //mstate.my_worker = worker_handle;
  mstate.worker_map[mstate.num_wokers] = worker_handle;
  mstate.num_wokers++;

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

  send_client_response(mstate.client_request_map[resp.get_tag()].waiting_client, resp);

  //remove itself from the queue
  if(!mstate.client_request_map.empty()){
    mstate.client_request_map.erase(resp.get_tag());
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

  //put into map first
  //DLOG(INFO) << "Put request into Queue! " << client_req.get_request_string() << std::endl;
  int tag = mstate.next_tag++;
  Request_msg worker_req(tag, client_req);

  Client_request client_request;
  client_request.request_msg = worker_req;
  client_request.waiting_client = client_handle;
  mstate.client_request_map[tag] = client_request;

  //send to worker now
  //DLOG(INFO) << "You are the only item in the Queue! process directly" << client_req.get_request_string() << std::endl;
  //Worker_handle worker_handle = mstate.worker_map[tag % mstate.num_wokers];
  send_request_to_worker(mstate.worker_map[tag % mstate.num_wokers], worker_req);
}


bool is_queue_too_long(){
  return true;
}

void handle_tick() {

  if(mstate.num_wokers<mstate.max_num_workers){
    //if queue is too long? how long? schedule another
    if(is_queue_too_long()){
      int tag = mstate.num_wokers+1;
      Request_msg req(tag);
      req.set_arg("name", "my worker "+tag);
      request_new_worker_node(req);
    }
  }
  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.
}

