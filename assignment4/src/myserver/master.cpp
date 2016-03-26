#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>

#include "server/messages.h"
#include "server/master.h"

#define INACTIVE 0
#define BOOTING 1
#define RUNNING 2
#define CLOSING 3

#define SCALE_OUT_THRESHOLD 36  // request per node > 36, scale out
#define SCALE_IN_THRESHOLD 32   // if scale in by one,   request per node <=32, then scale in

//job type
#define COMPUTE 0
#define PROJECTIDEA 1
#define TELLMENOW 2

using namespace std;

// put all requests in a queue, keep track of waiting client so that we know which client to return
// look up this in the queue/map using the tag value (unique indicator)
struct Client_request{
  Request_msg request_msg;
  Client_handle waiting_client;
};

struct Worker_info{
  Worker_handle worker_handle;
  int status;
  int compute_jobs;
  int tellmenow_jobs;
  int projectidea_jobs;
};

class Worker_metrics{
  public:
    int max_num_workers;
    map<int, Worker_info> worker_info_map;
  public:
    void init(int number){
      max_num_workers = number;
      for(int i = 0; i< max_num_workers; i++){
        Worker_info worker_info;
        worker_info.status=INACTIVE;
        worker_info_map[i]=worker_info;
      }
    }
    int getScaleNumber(){
      //count requests number per worker (booting + running)
      int total_request = 0; //exclude tellmenow jobs
      int running = 0;
      int booting = 0;
      int closing = 0;
      for(int i = 0; i< max_num_workers; i++){
        if(worker_info_map[i].status == RUNNING){
          running++;
          total_request+=(worker_info_map[i].compute_jobs+worker_info_map[i].projectidea_jobs);
        }else if(worker_info_map[i].status == BOOTING) {
          booting++;
          total_request+=(worker_info_map[i].compute_jobs+worker_info_map[i].projectidea_jobs);
        }
        else if(worker_info_map[i].status == CLOSING){
          closing++;
        }
      }

      //DLOG(INFO) << "Get Scale Number: running="<<running <<", booting="<<booting<<", closing="<<closing<<", total request="<<total_request << std::endl;

      if((running+booting)==0){
        return 0;
      }
      if (total_request/(running+booting)>SCALE_OUT_THRESHOLD){
        int result =  ((total_request+SCALE_IN_THRESHOLD-1) / SCALE_IN_THRESHOLD) - (running+booting);
        if(result+running+booting>max_num_workers)
          result=max_num_workers-running-booting;
        return result;

      } else {
        if((running+booting-1)==0)
          return 0;
        else if(total_request/(running+booting-1)>=SCALE_IN_THRESHOLD) {
          return -1;
        }
        return 0;
      }
    }
    int getFirstWorker(int status){
      for(int i = 0; i< max_num_workers; i++){
        if(worker_info_map[i].status == status)
          return i;
      }
      return -1;
    }
    void scaleOut(int number) {
      //scale out number of instances
      for(int i = 0; i< number; i++){
        //check if there are closing worker
        int closing_tag = getFirstWorker(CLOSING);
        if(closing_tag>=0){
          worker_info_map[closing_tag].status = RUNNING;
          continue;
        }
        //I have ensured this tag will be larger than 0;
        int tag = getFirstWorker(INACTIVE);
        worker_info_map[tag].status = BOOTING;
        Request_msg req(tag);
        req.set_arg("name", "my worker "+to_string(tag));
        request_new_worker_node(req);
        DLOG(INFO) << "worker "<<tag<<" is booting!" << std::endl;
      }
    }
    void scaleIn(int number) {
      //scale in number of instances, mark as closing
      for(int i = 0; i< number; i++){
        int tag = getFirstWorker(RUNNING);
        worker_info_map[tag].status = CLOSING;
      }
    }
    void actualShutDown(){
      for(int i = 0; i< max_num_workers; i++){
        if(worker_info_map[i].status == CLOSING){
          if(worker_info_map[i].compute_jobs==0
              && worker_info_map[i].tellmenow_jobs==0
              && worker_info_map[i].projectidea_jobs==0){
            //actual show down
            kill_worker_node(worker_info_map[i].worker_handle);
          }
        }
      }
    }
    void markWorkOnline(Worker_handle worker_handle, int tag){
      worker_info_map[tag].worker_handle = worker_handle;
      worker_info_map[tag].status = RUNNING;
    }
    void updateWorkerJobs(int job_type, int index, int amount){
      if(job_type==COMPUTE){
        worker_info_map[index].compute_jobs+=amount;
      }else if (job_type==PROJECTIDEA){
        worker_info_map[index].projectidea_jobs+=amount;
      }else{//tell me now
        worker_info_map[index].tellmenow_jobs+=amount;
      }
    }
    void sendWork(const Request_msg& req){
      int min_index=-1;
      int min_count=800;
      int job_type =COMPUTE;
      if(req.get_arg("cmd").compare("projectidea")==0)
        job_type=PROJECTIDEA;
      else if(req.get_arg("cmd").compare("tellmenow")==0)
        job_type=TELLMENOW;

      for(int i = 0; i< max_num_workers; i++){
        if(worker_info_map[i].status==RUNNING){
          if(job_type==COMPUTE){
            if(worker_info_map[i].compute_jobs<=min_count){
              min_count = worker_info_map[i].compute_jobs;
              min_index=i;
            }
          }else if (job_type==PROJECTIDEA){
            if(worker_info_map[i].projectidea_jobs<=min_count){
              min_count = worker_info_map[i].projectidea_jobs;
              min_index=i;
            }
          }else{//tell me now
            if(worker_info_map[i].tellmenow_jobs<=min_count){
              min_count = worker_info_map[i].tellmenow_jobs;
              min_index=i;
            }
          }
        }
      }

      updateWorkerJobs(job_type,min_index, 1);
      DLOG(INFO) << "LB: sending work type["<<job_type<<"] to least busy RUNNING worker ["<<min_index<<"]" << std::endl;
      send_request_to_worker(worker_info_map[min_index].worker_handle, req);
    }
    void handleWorkResp(string cmd, const Response_msg& resp){
      int job_type =COMPUTE;
      if(cmd.compare("projectidea")==0)
        job_type=PROJECTIDEA;
      else if(cmd.compare("tellmenow")==0)
        job_type=TELLMENOW;
      int index = resp.get_tag();
      updateWorkerJobs(job_type,index,-1);
    }
};


static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int next_tag;

  //defines workers to handle the work
  Worker_metrics worker_metrics;

  //a queue to store the requests
  map<int,Client_request> client_request_map;

} mstate;


void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 1;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;

  mstate.worker_metrics.init(mstate.max_num_workers);

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  DLOG(INFO) << "Scaling the first worker" << std::endl;
  mstate.worker_metrics.scaleOut(1);

  DLOG(INFO) << "Initialized master...max number of worker is "<< max_workers << std::endl;

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  DLOG(INFO) << "worker "<<tag<<" is running!" << std::endl;
  mstate.worker_metrics.markWorkOnline(worker_handle, tag);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
    DLOG(INFO) << "Server is ready!" << std::endl;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  send_client_response(mstate.client_request_map[resp.get_tag()].waiting_client, resp);


  string cmd = mstate.client_request_map[resp.get_tag()].request_msg.get_arg("cmd");

  //remove itself from the queue
  if(!mstate.client_request_map.empty()){
    mstate.client_request_map.erase(resp.get_tag());
  }

  //update number of jobs in work info
  mstate.worker_metrics.handleWorkResp(cmd, resp);
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
  //now send based on MOD value. Actually, in the future, this could be done through
  //DLOG(INFO) << "You are the only item in the Queue! process directly" << client_req.get_request_string() << std::endl;

  mstate.worker_metrics.sendWork(worker_req);
  //send_request_to_worker(mstate.worker_map[tag % mstate.num_wokers], worker_req);
}

void handle_tick() {
  if(mstate.server_ready) {
    //DLOG(INFO) << "TICK!" << std::endl;
    int scale_number = mstate.worker_metrics.getScaleNumber();
    //DLOG(INFO) << "Scale Number "<< scale_number << std::endl;
    if (scale_number > 0) {
      DLOG(INFO) << "Scale OUT EVENT! Number "<< scale_number << std::endl;
      mstate.worker_metrics.scaleOut(scale_number);
    } else if (scale_number < 0) {
      DLOG(INFO) << "Scale IN EVENT! Number "<< -1*scale_number << std::endl;
      mstate.worker_metrics.scaleIn(-1 * scale_number);
    }
    mstate.worker_metrics.actualShutDown();
    //DLOG(INFO) << "TICKed!" << std::endl;
  }
}

