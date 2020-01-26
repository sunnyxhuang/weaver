//
//  traffic_generator.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/21/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#include <iomanip>
#include <algorithm> // needed for find in NotifyTrafficFinish()
#include <ctgmath>
#include <numeric> //dbg
#include <random>

#include "coflow.h"
#include "events.h"
#include "traffic_generator.h"
#include "util.h"

using namespace std;

TrafficGen::TrafficGen() {
  m_currentTime = 0;
  m_simPtr = NULL;
  m_myTimeLine = new TrafficGenTimeLine;

  m_totalCCT = 0;
  m_totalFCT = 0;

  m_totalCoflowNum = 0;
  m_total_accepted_coflow_num = 0;
  m_total_met_deadline_num = 0;
  db_logger_ = nullptr;
}

TrafficGen::~TrafficGen() {

  delete m_myTimeLine;

  cout << "Done" << " ";
  cout << m_total_met_deadline_num
       << "/" << m_total_accepted_coflow_num
       << "/" << m_totalCoflowNum << " ";
  cout << "" << m_totalCCT << " ";
  cout << "" << m_totalFCT << " ";
  cout << endl;

  if (m_jobTraceFile.is_open()) {
    m_jobTraceFile.close();
  }
  if (m_cctAuditFile.is_open()) {
    m_cctAuditFile << "Done" << " ";
    m_cctAuditFile << m_total_met_deadline_num
                   << "/" << m_total_accepted_coflow_num
                   << "/" << m_totalCoflowNum << " ";
    m_cctAuditFile << "" << m_totalCCT << " ";
    m_cctAuditFile << "" << m_totalFCT << " ";
    m_cctAuditFile << endl;
    m_cctAuditFile.close();
  }
  if (m_fctAuditFile.is_open()) {
    m_fctAuditFile.close();
  }
  for (Coflow* coflow : coflows_saved_) {
    delete coflow;
  }
}

void
TrafficGen::UpdateAlarm() {
  if (!m_myTimeLine->isEmpty()) {
    Event* nextEvent = m_myTimeLine->PeekNext();
    double nextTime = nextEvent->GetEventTime();
    Event* ep = new Event(ALARM_TRAFFIC, nextTime);
    m_simPtr->UpdateTrafficAlarm(ep);
  }
}

////////////////////////////////////////////////////
///////////// Code for FB Trace Replay   ///////////
////////////////////////////////////////////////////

TGTraceFB::TGTraceFB(DbLogger* db_logger) {

  db_logger_ = db_logger;

  m_readyJob = vector<JobDesc*>();
  m_runningJob = vector<JobDesc*>();

  m_coflow2job = map<Coflow*, JobDesc*>();

  // coflow trace
  m_jobTraceFile.open(TRAFFIC_TRACE_FILE_NAME);
  if (!m_jobTraceFile.is_open()) {
    cout << "Error: unable to open file "
         << TRAFFIC_TRACE_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }

  // output files
  m_cctAuditFile.open(CCT_AUDIT_FILE_NAME);
  if (!m_cctAuditFile.is_open()) {
    cout << "Error: unable to open file "
         << CCT_AUDIT_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }

  m_fctAuditFile.open(FCT_AUDIT_FILE_NAME);
  if (!m_fctAuditFile.is_open()) {
    cout << "Error: unable to open file "
         << FCT_AUDIT_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }

  int seed_for_perturb_seed = 13;
  InitSeedForCoflows(seed_for_perturb_seed);

}

TGTraceFB::~TGTraceFB() {
  if (!m_runningJob.empty()) {
    //error
    cout << "[TGTraceFB::~TGTraceFB()] Error: "
         << "has unfinished traffic at the end of simulation!\n";
    for (JobDesc* job: m_runningJob) {
      Coflow* coflow = job->m_coflow;
      cout << coflow->GetName() << " is NOT finished.\n";
      for (Flow* flow: *coflow->GetFlows()) {
        if (flow->HasDemand()) {
          cout << coflow->GetName() << "  " << flow->toString()
               << " elec_rate " << flow->GetElecRate() << endl;
        }
      }
    }
  }
}

/* called by simulator */
void
TGTraceFB::NotifySimStart() {
  vector<JobDesc*> jobs2add = ReadJobs();
  ScheduleToAddJobs(jobs2add);
  UpdateAlarm();
}

void
TGTraceFB::TrafficGenAlarmPortal(double alarmTime) {

  while (!m_myTimeLine->isEmpty()) {
    Event* currentEvent = m_myTimeLine->PeekNext();
    double currentEventTime = currentEvent->GetEventTime();

    if (currentEventTime > alarmTime) {
      // break if we have over run the clock
      break;
    }
    // it seems everything is ok
    m_currentTime = alarmTime;

//    cout << fixed << setw(FLOAT_TIME_WIDTH) << currentEventTime << "s "
//         << "[TGTraceFB::TrafficGenAlarmPortal] "
//         << " working on event type " << currentEvent->GetEventType() << endl;

    //currentEvent->CallBack();
    switch (currentEvent->GetEventType()) {
      case SUB_JOB:DoSubmitJob();
        break;
      default:break;
    }

    Event* e2p = m_myTimeLine->PopNext();
    if (e2p != currentEvent) {
      cout << "[TGTraceFB::SchedulerAlarmPortal] error: "
           << " e2p != currentEvent " << endl;
    }
    delete currentEvent;
  }

  UpdateAlarm();
}

// Called by simulator. The Scheduler class adds an event in the simulator.
// The simulator event invokes this function.
void
TGTraceFB::NotifyTrafficFinish(double alarm_time,
                               vector<Coflow*>* coflows_ptr,
                               vector<Flow*>* flows_ptr) {

  for (Flow* flow : *flows_ptr) {
    if (flow->GetEndTime() != INVALID_TIME
        && flow->GetEndTime() >= flow->GetStartTime()) {
      // This flow is properly done.
      // A valid start/end time => count FCT
      m_totalFCT += (flow->GetEndTime() - flow->GetStartTime());
      if (db_logger_) {
        db_logger_->WriteOnFlowFinish(alarm_time, flow, m_fctAuditFile);
      }
    } else {
      // sth is wrong.
      cout << "Error while calculating flow end time: invalid start/end time\n";
      exit(-1);
    }
  }

  for (Coflow* coflow : *coflows_ptr) {
    // count coflow num no matter what.
    m_totalCoflowNum++;

    if (coflow->IsRejected()) {
      // only count completed coflow.
      // do not consider rejected coflow.
      continue;
    }
    m_total_accepted_coflow_num++;

    double cct = INVALID_TIME;

    if (coflow->GetEndTime() != INVALID_TIME
        && coflow->GetEndTime() >= coflow->GetStartTime()) {
      // This coflow is properly finished.
      // a valid end time => count CCT
      cct = coflow->GetEndTime() - coflow->GetStartTime();
      if (db_logger_) {
        db_logger_->WriteCoflowFeatures(coflow);
        db_logger_->WriteOnCoflowFinish(alarm_time, coflow, m_cctAuditFile);
      }
    } else {
      // sth is wrong.
      cout << " error while calculating coflow end time: invalid start/end time"
           << endl;
      exit(-1);
    }

    if (coflow->GetDeadlineSec() <= 0) {
      // no deadline.
      m_totalCCT += cct;
      jobid_to_CCT_[coflow->GetJobId()] = cct;
    } else {
      // consider deadline.
      if (cct <= (coflow->GetDeadlineSec() + DEADLINE_ERROR_TOLERANCE)) {
        // met deadline.
        // only count CCT on accepted coflows.
        m_total_met_deadline_num++;
        m_totalCCT += cct;
      }
    }
  }

  // remove the job and coflows
  for (Coflow* coflow : *coflows_ptr) {
    map<Coflow*, JobDesc*>::iterator jobmapIt = m_coflow2job.find(coflow);
    if (jobmapIt == m_coflow2job.end()) {
      cout << "error: can't locate the job from the coflow ptr" << endl;
      cout << "return without clearing up the dead job" << endl;
      return;
    }
    JobDesc* jp2rm = jobmapIt->second;

    if (TEST_ONLY_SAVE_COFLOW_AFTER_FINISH) {
      coflows_saved_.push_back(coflow); // Also deletes the flow pointers under this coflow.
    } else {
      delete coflow; // Also deletes the flow pointers under this coflow.
    }
    delete jp2rm;

    m_coflow2job.erase(jobmapIt);

    vector<JobDesc*>::iterator runJIt = find(m_runningJob.begin(),
                                             m_runningJob.end(),
                                             jp2rm);

    if (runJIt == m_runningJob.end()) {
      cout << "Error: the job to delete is not found in the running jobs!\n";
    } else {
      m_runningJob.erase(runJIt);
      m_finishJob.push_back(jp2rm);
    }
  }
}

void
TGTraceFB::NotifySimEnd() {

}

void
TGTraceFB::DoSubmitJob() {

  KickStartReadyJobsAndNotifyScheduler();

  // add next job
  vector<JobDesc*> nextjobs2add = ReadJobs();
  ScheduleToAddJobs(nextjobs2add);
}

// We assume one coflow per event, per job.
void
TGTraceFB::KickStartReadyJobsAndNotifyScheduler() {

  EventSubmitJobDesc* ep = (EventSubmitJobDesc*) m_myTimeLine->PeekNext();
  if (ep->GetEventType() != SUB_JOB) {
    cout << "[TGTraceFB::DoSubmitJob] error: "
         << " the event type is not SUB_JOB!" << endl;
    return;
  }

  vector<JobDesc*>::iterator jIt = find(m_readyJob.begin(),
                                        m_readyJob.end(),
                                        ep->m_jobPtr);

  if (jIt == m_readyJob.end()) {
    cout << "error: the job to submit is not in the "
         << " ready job set!" << endl;
  } else {
    m_readyJob.erase(jIt);
    m_runningJob.push_back(ep->m_jobPtr);
  }

  // dump traffic into network
  vector<Coflow*>* msgCfVp = new vector<Coflow*>();
  msgCfVp->push_back(ep->m_jobPtr->m_coflow);
  MsgEventAddCoflows* msgEp = new MsgEventAddCoflows(m_currentTime, msgCfVp);
  m_simPtr->AddEvent(msgEp);
}

void
TGTraceFB::ScheduleToAddJobs(vector<JobDesc*>& jobs_to_add) {
  if (jobs_to_add.empty()) return;
  for (vector<JobDesc*>::iterator jobIt = jobs_to_add.begin();
       jobIt != jobs_to_add.end(); jobIt++) {
    EventSubmitJobDesc* ep =
        new EventSubmitJobDesc((*jobIt)->m_offArrivalTime, *jobIt);
    m_myTimeLine->AddEvent(ep);
    m_readyJob.push_back(*jobIt);
  }
}

// read next job(s)
// the last job trace line
// should end with return
vector<JobDesc*>
TGTraceFB::ReadJobs() {

  vector<JobDesc*> result;

  string jobLine = "";

  long firstJobTime = -1;

  while (!m_jobTraceFile.eof()) {

    getline(m_jobTraceFile, jobLine);

    if (jobLine.size() <= 0) {
      // return after reading all lines in the file
      //cout << "no more jobs are available!" << endl;
      return result;
    }

    //    vector<string> subFields;
    //    long numFields = split(jobLine, subFields, '\t');
    //    assert(numFields == 5);

    std::istringstream ss(jobLine);
    vector<std::string> fields(5);
    if (!(ss >> fields[0] >> fields[1] >> fields[2] >> fields[3]
             >> fields[4])) {
      return result;
    }

    long jobOffArrivalTimel = stol(fields[1]);
    int jobid = stoi(fields[0]);
    // jobOffArrivalTime in seconds.
    double jobOffArrivalTime = stod(fields[1])
        / 1000.0 / TRAFFIC_ARRIVAL_SPEEDUP;
    int map = stoi(fields[2]);
    int red = stoi(fields[3]);

    // if ENABLE_PERTURB_IN_PLAY = true, perturb flow sizes.
    // if EQUAL_FLOW_TO_SAME_REDUCER = true, all flows to the same reducer
    //     will be the of the same size.
    Coflow* cfp = CreateCoflowPtrFromString(jobOffArrivalTime, jobid,
                                            map, red, fields[4],
                                            ENABLE_PERTURB_IN_PLAY,
                                            EQUAL_FLOW_TO_SAME_REDUCER);
    if (cfp) {
      if (firstJobTime < 0) {
        firstJobTime = jobOffArrivalTimel;
      }

      if (jobOffArrivalTimel > firstJobTime) {
        // return because the next job has exceed the current jobs' arrival time
        //seek back file seeker and return
        m_jobTraceFile.seekg(-(jobLine.length() + 1), m_jobTraceFile.cur);
        return result;
      }

      int num_flow = (int) cfp->GetFlows()->size();
      JobDesc* newJobPtr = new JobDesc(jobid,
                                       jobOffArrivalTime,
                                       map, red, num_flow, cfp);
      // add entry into the map
      m_coflow2job.insert(pair<Coflow*, JobDesc*>(cfp, newJobPtr));
      result.push_back(newJobPtr);
    }
  }
  // return after reading all lines in the file
  return result;
}

class ResourceRequestGenerator {
 public:
  ResourceRequestGenerator(const double rv_values[],
                           const int rv_prob[],
                           int sample_num, int seed) :
      rv_values_(std::vector<double>(rv_values, rv_values + sample_num)),
      rv_prob_(std::vector<int>(rv_prob, rv_prob + sample_num)),
      distribution_(std::discrete_distribution<int>(
          rv_prob_.begin(), rv_prob_.end())),
      generator_(seed) {}
  double Rand() {
    return rv_values_[distribution_(generator_)];
  }
 private:
  const std::vector<double> rv_values_;
  const std::vector<int> rv_prob_;
  std::mt19937 generator_;
  std::discrete_distribution<int> distribution_;
};

Coflow*
TGTraceFB::CreateCoflowPtrFromString(double time, int coflow_id,
                                     int num_map, int num_red,
                                     string cfInfo,
                                     bool do_perturb, bool avg_size) {
  //  cout << "[TGTraceFB::CreateCoflowPtrFromString] "
  //       << "Creating coflow #"<< coflow_id << endl;

  vector<string> subFields;
  if (split(cfInfo, subFields, '#') != 2) {
    cout << __func__ << ": number of fields illegal!"
         << "Return with NULL coflow ptr." << endl;
    return NULL;
  }

  // Obtain traffic requirements.
  // map from (mapper_idx, reducer_idx) to flow size in bytes. Mappers and
  // reducers are virtually indexed within this coflow. The actual placement of
  // the mapper and reducer tasks are to be determined.
  vector<int> mapper_original_locations, reducer_original_locations;
  mapper_original_locations = GetMapperOriginalLocFromString(num_map,
                                                             subFields[0]);
  vector<long> reducer_input_bytes;
  GetReducerInfoFromString(num_red, subFields[1],
                           &reducer_original_locations,
                           &reducer_input_bytes);

  map<pair<int, int>, long> mr_flow_bytes;
  if (!do_perturb) {
    if (avg_size) {
      mr_flow_bytes = GetFlowSizeWithEqualSizeToSameReducer(
          num_map, num_red, reducer_input_bytes);
    } else {
      mr_flow_bytes = GetFlowSizeWithExactSize(
          num_map, num_red, reducer_input_bytes);
    }
  } else {
    // let us allow some random perturbation.
    mr_flow_bytes = GetFlowSizeWithPerturb(
        num_map, num_red, reducer_input_bytes,
        5/* hard code of +/-5% */, GetSeedForCoflow(coflow_id));
  }
  if (mr_flow_bytes.size() != num_map * num_red) {
    cout << "Error: The number of flows does not match. Exit with error.\n";
    exit(-1);
  }
  if (TRAFFIC_SIZE_INFLATE != 1.0) {
    for (auto& pair: mr_flow_bytes) {
      pair.second *= TRAFFIC_SIZE_INFLATE;
      if (pair.second < 1e6) {
        pair.second = 1e6; // all flows >= 1MB;
      }
    }
  }
  // Use current usage and the traffic/resource requirements to place mapper and
  // reducer tasks.
  // For small coflows whose (num_map + num_red < NUM_RACKS), we further requires
  // all mappers and reducers are on different nodes.
  // For the rest large coflows, we assume all mappers (reducers) are on
  // different nodes. However, mapper and reducer may share the same node, i.e.
  // flows may have the same src and dst.
  // 1/2 place mapper tasks
  vector<int> mapper_locations, reducer_locations;
  PlaceTasks(coflow_id,
             num_map,
             num_red,
             mr_flow_bytes,
             mapper_original_locations,
             reducer_original_locations,
             &mapper_locations,
             &reducer_locations);

  if (mapper_locations.size() != num_map
      || reducer_locations.size() != num_red) {
    cout << "Error while assigning machine locations. "
         << "Size of placemnet does not match. Exit with error.\n";
    exit(-1);
  }

  // create the coflow based on the coflow configs
  Coflow* coflow = CreateCoflow(coflow_id, time,
                                num_map, num_red, mr_flow_bytes,
                                mapper_locations, reducer_locations);

  if (!coflow) {
    return nullptr;
  }

  // generate a deadline here if needed!
  if (DEADLINE_MODE) {
    double lb_optc = coflow->GetMaxOptimalWorkSpanInSeconds();
    double lb_elec
        = ((double) coflow->GetLoadOnMaxOptimalWorkSpanInBits()) / ELEC_BPS;
    unsigned int rand_seed = GetSeedForCoflow(coflow_id);
    std::mt19937 mt_rand(rand_seed); // srand(rand_seed);
    // currently we assume the inflation x = 1;
    double deadline
        = lb_optc + lb_optc * (((double) (mt_rand() % 100)) / 100.0);
    coflow->SetDeadlineSec(deadline);
    if (DEBUG_LEVEL >= 10) {
      cout << " lb_elec " << lb_elec << " lb_optc " << lb_optc
           << " deadline " << deadline << endl;
    }
  }

  return coflow;
}

Coflow* TGTraceFB::CreateCoflow(int coflow_id, double arrival_time,
                                int num_map, int num_red,
                                const map<pair<int, int>, long>& mr_flow_bytes,
                                const vector<int>& mapper_locations,
                                const vector<int>& reducer_locations) {
  // Create flows by marrying the placement decisions with traffic requirements.
  vector<Flow*> flows;
  for (const auto& kv_pair : mr_flow_bytes) {
    int mapper_idx = kv_pair.first.first;
    int reducer_idx = kv_pair.first.second;
    long flow_bytes = kv_pair.second;
    int src = mapper_locations[mapper_idx];
    int dst = reducer_locations[reducer_idx];
    if (!REMOTE_IN_OUT_PORTS && src == dst) {
      // we choose not to add traffic with the same source and destination so
      // as to avoid error in scheduler.
      continue;
    }
    flows.push_back(new Flow(arrival_time, src, dst, flow_bytes));
  }

  if (flows.empty()) {
    // this coflow has no demand at all.
    return nullptr;
  }

  Coflow* coflow = new Coflow(arrival_time);
  for (Flow* flow : flows) {
    coflow->AddFlow(flow);
    flow->SetParentCoflow(coflow);
  }
  coflow->SetPlacement(mapper_locations, reducer_locations);
  coflow->SetMRFlowBytes(mr_flow_bytes);
  // initialize the static alpha upon creation.
  coflow->SetStaticAlpha(coflow->CalcAlpha());

  // Calculate max traffic load requested on individual mapper or reducer node.
  // This metric will NOT change with different placement.
  vector<double> mapper_traffic_req_MB, reducer_traffic_req_MB;
  GetNodeReqTrafficMB(num_map, num_red, mr_flow_bytes,
                      &mapper_traffic_req_MB, &reducer_traffic_req_MB);
  coflow->SetMapReduceLoadMB(mapper_locations, reducer_locations,
                             mapper_traffic_req_MB, reducer_traffic_req_MB);
  coflow->SetJobId(coflow_id);
  coflow->coflow_rand_seed_ = GetSeedForCoflow(coflow_id);
  return coflow;
}

vector<int>
TGTraceFB::GetMapperOriginalLocFromString(int num_mapper_wanted,
                                          const string& mapper_trace) {
  vector<string> locations;
  long num_fields = split(mapper_trace, locations, ',');
  if (num_fields != num_mapper_wanted) {
    cout << "ERROR: num_fields != num_mapper_wanted. "
         << "Return empty assignments." << endl;
    return vector<int>();
  }
  vector<int> result;
  for (const string& loc : locations) {
    result.push_back(std::stoi(loc));
  }
  return result;
}

void TGTraceFB::GetReducerInfoFromString(int num_reducer_wanted,
                                         const string& reducer_trace,
                                         vector<int>* original_locations,
                                         vector<long>* reducer_input_bytes) {
  original_locations->clear();
  reducer_input_bytes->clear();

  vector<string> reducer_info;
  int num_fields = split(reducer_trace, reducer_info, ',');
  if (num_fields != num_reducer_wanted) {
    cout << __func__ << ": num_fields != num_reducer_wanted. "
         << "Return empty assignments." << endl;
    return;
  }
  for (int reducer_idx = 0; reducer_idx < num_reducer_wanted; reducer_idx++) {
    vector<string> reducer_input_pair;
    int num_sub_fields = split(reducer_info[reducer_idx],
                               reducer_input_pair, ':');
    if (num_sub_fields != 2) {
      cout << __func__ << ": num_sub_fields != 2. Return empty assignments.\n";
      return;
    }
    original_locations->push_back(std::stoi(reducer_input_pair[0]));
    reducer_input_bytes->push_back(1000000 * stol(reducer_input_pair[1]));
  }
}

void TGTraceFB::GetNodeReqTrafficMB(
    int num_map, int num_red,
    const map<pair<int, int>, long>& mr_flow_bytes,
    vector<double>* mapper_traffic_req_MB,
    vector<double>* reducer_traffic_req_MB) {
  std::vector<double>(num_map).swap(*mapper_traffic_req_MB);
  std::vector<double>(num_red).swap(*reducer_traffic_req_MB);
  for (const auto& kv_pair: mr_flow_bytes) {
    double req_MB = (double) kv_pair.second / 1e6;
    int mapper_idx = kv_pair.first.first;
    mapper_traffic_req_MB->operator[](mapper_idx) += req_MB;
    int reducer_idx = kv_pair.first.second;
    reducer_traffic_req_MB->operator[](reducer_idx) += req_MB;
  }
}

// use seed_for_seed to initialize the vector of m_seed_for_coflow,
// in length of PERTURB_SEED_NUM.
void TGTraceFB::InitSeedForCoflows(int seed_for_seed) {
  std::mt19937 mt_rand(seed_for_seed); //  srand(seed_for_seed);
  for (int i = 0; i < PERTURB_SEED_NUM; i++) {
    m_seed_for_coflow.push_back(mt_rand()); // m_seed_for_coflow.push_back(rand());
    // cout << " seed [" << i << "] = " << m_seed_for_coflow.back() << endl;
  }
}

int
TGTraceFB::GetSeedForCoflow(int coflow_id) {
  // use jobid to identify a unique seed;
  int seed_idx = (coflow_id + 5122) % PERTURB_SEED_NUM;
  int seed = m_seed_for_coflow[seed_idx];
  // cout << "job id " << coflow_id << " seed " << seed << endl;
  return seed;
}

// sum( flows to a reducer ) == reducer's input specified in redInput.
map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithExactSize(int numMap,
                                    int numRed,
                                    const vector<long>& redInput) {
  map<pair<int, int>, long> mr_flow_bytes_result;
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long redInputTmp = redInput[reducer_idx];
    long avgFlowSize = ceil((double) redInputTmp / (double) numMap);
    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {
      long flowSize = min(avgFlowSize, redInputTmp);
      redInputTmp -= flowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;
    }
  }
  return mr_flow_bytes_result;
}

// divide reducer's input size, specified in redInput, to each of the mapper.
map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithEqualSizeToSameReducer(int numMap,
                                                 int numRed,
                                                 const vector<long>& redInput) {
  map<pair<int, int>, long> mr_flow_bytes_result;
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long avgFlowSize = ceil((double) redInput[reducer_idx] / (double) numMap);
    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {
      long flowSize = avgFlowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;
    }
  }
  return mr_flow_bytes_result;
}

map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithPerturb(int numMap,
                                  int numRed,
                                  const vector<long>& redInput,
                                  int perturb_perc,
                                  unsigned int rand_seed) {

  if (perturb_perc > 100) {
    cout << " Warming : try to perturb the flow sizes with more than 1MB \n";
  }
  // seed the random generator before we preturb,
  // so that given same traffic trace,
  // we will have the same traffic for different schedulers.
  std::mt19937 mt_rand(rand_seed); // srand(rand_seed);

  map<pair<int, int>, long> mr_flow_bytes_result;
  // now we generate traffic.
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long redInputTmp = redInput[reducer_idx];

    long avgFlowSize = ceil((double) redInputTmp / (double) numMap);

    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {

      int perturb_direction = (mt_rand() % 2 == 1) ? 1 : -1;
      // int perturb_direction = (rand() % 2 == 1) ? 1 : -1;

      // perturb_perc = 5 : (-5%, +5%) flow size , exclusive bound

      double rand_0_to_1 = ((double) mt_rand() / (RAND_MAX));
      // double rand_0_to_1 = ((double) rand() / (RAND_MAX));
      double perturb_perc_rand =
          perturb_direction * rand_0_to_1 * (double) perturb_perc / 100.0;
      long flowSize = avgFlowSize * (1 + perturb_perc_rand);

      // only allow flows >= 1MB.
      if (flowSize < 1000000) flowSize = 1000000;
      redInputTmp -= flowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;

      // debug
      if (DEBUG_LEVEL >= 10) {
        cout << "mapper " << mapper_idx << " -> reducer " << reducer_idx << ", "
             << "flow size after perturb " << flowSize << ", "
             << "avgFlowSize " << avgFlowSize << ", "
             << "perturb_perc_rand " << perturb_perc_rand << endl;
      }
    }
  }
  return mr_flow_bytes_result;
}


///////////////////////////////////////////////////////////////////
///////////// Code for back-to-back coflow              ///////////
///////////// Replay FB trace but ignore real arrival time  ///////
///////////// so that one coflow is served at a time.     /////////
///////////////////////////////////////////////////////////////////

TGFBOnebyOne::TGFBOnebyOne(DbLogger* db_logger)
    : TGTraceFB(db_logger/*db_logger*/) {
  m_last_coflow_finish_time = 0.0;

  m_comptimeAuditFile.open(COMPTIME_AUDIT_FILE_NAME);
  if (!m_comptimeAuditFile.is_open()) {
    cout << "Error: unable to open file "
         << COMPTIME_AUDIT_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }

  m_comptime_audit_title_line = false;
}

TGFBOnebyOne::~TGFBOnebyOne() {
  if (m_comptimeAuditFile.is_open()) {
    m_comptimeAuditFile << "Done" << endl;
    m_comptimeAuditFile.close();
  }
}

void
TGFBOnebyOne::DoSubmitJob() {
  TGTraceFB::KickStartReadyJobsAndNotifyScheduler();
}

// do not apply any destruction here!
void
TGFBOnebyOne::LogComptimeStat(vector<Coflow*>* cfpVp) {
  if (cfpVp->size() != 1) {
    // should only have 1 coflow finished at a time.
    return;
  }
  // obtain comp time stats.
  int jobid = cfpVp->front()->GetJobId();
  CompTimeBreakdown comptime = cfpVp->front()->GetCompTime();

  // look up the job for more information.
  map<Coflow*, JobDesc*>::iterator
      jobmapIt = m_coflow2job.find(cfpVp->front());
  if (jobmapIt == m_coflow2job.end()) {
    cout << "error: can't locate the job from the coflow ptr" << endl;
    cout << "return without clearing up the dead job" << endl;
    return;
  }
  JobDesc* job = jobmapIt->second;
  int num_map = job->m_numSrc;
  int num_red = job->m_numDst;

  if (!m_comptime_audit_title_line) {
    m_comptime_audit_title_line = true;
    m_comptimeAuditFile
        << "jobid" << '\t'
        << "map" << '\t' << "red" << '\t'
        << "sun-ttl" << '\t'
        << "shuffleR" << '\t' << "shuffleS" << '\t'
        << "reserve" << '\t'
        << "solstice-ttl" << '\t'
        << "stuff" << '\t' << "slice" << '\t'
        << "vec-avg" << '\t' << "count" << '\t' << "min" << '\t' << "max"
        << '\t'
        << endl;
  }
  m_comptimeAuditFile
      << jobid << '\t'
      << num_map << '\t'
      << num_red << '\t'
      << comptime.GetSunflowTotalTime() << '\t'
      << comptime.m_time_shuffle_random << '\t'
      << comptime.m_time_sort << '\t'
      << comptime.m_time_reserve << '\t'
      << comptime.GetSolsticeTotalTime() << '\t'
      << comptime.m_time_stuff << '\t'
      << comptime.m_time_slice << '\t'
      << comptime.GetVectorAvgTime() << '\t'
      << comptime.m_time_vector.size() << '\t'
      << comptime.GetVectorMinTime() << '\t'
      << comptime.GetVectorMaxTime() << '\t'
      << endl;
}

void
TGFBOnebyOne::NotifyTrafficFinish(double alarmTime,
                                  vector<Coflow*>* cfpVp,
                                  vector<Flow*>* fpVp) {


  // only output comp time when we play with 1by1.
  // log comptime before calling TGTraceFB::NotifyTrafficFinish()
  // as coflow pointers are destroyed in TGTraceFB::NotifyTrafficFinish().
  LogComptimeStat(cfpVp);

  TGTraceFB::NotifyTrafficFinish(alarmTime, cfpVp, fpVp);

  // add another coflow if any.
  if (!cfpVp->empty()) {
    // we have coflow finish. update last finish time.
    m_last_coflow_finish_time = alarmTime;

    // see whether we have more coflows.
    vector<JobDesc*> nextjob2add = ReadJobs();
    TGTraceFB::ScheduleToAddJobs(nextjob2add);

    // update alarm for scheduler.
    UpdateAlarm();
  }

}

vector<JobDesc*> TGFBOnebyOne::ReadJobs() {

  vector<JobDesc*> result;
  Coflow* coflow = nullptr;
  while (!coflow) {
    string jobLine = "";
    getline(m_jobTraceFile, jobLine);

    if (jobLine.size() <= 0) {
      //cout << "no more jobs are available!" << endl;
      return result;
    }

    std::istringstream ss(jobLine);
    vector<std::string> fields(5);
    if (!(ss >> fields[0] >> fields[1] >> fields[2] >> fields[3]
             >> fields[4])) {
      cout << "[TGFBOnebyOne::ReadJobs] number of fields illegal! "
           << "Return with job list." << endl;
      return result;
    }

    int jobid = stoi(fields[0]);
    double jobOffArrivalTime = m_last_coflow_finish_time;
    int map = stoi(fields[2]);
    int red = stoi(fields[3]);

    // perform perturb if needed.
    // when perturb = false,
    //  if EQUAL_FLOW_TO_SAME_REDUCER = true, all flows to the same reducer
    //     will be the of the same size.

    coflow = CreateCoflowPtrFromString(jobOffArrivalTime, jobid,
                                       map, red, fields[4],
                                       ENABLE_PERTURB_IN_PLAY,
                                       EQUAL_FLOW_TO_SAME_REDUCER);
    if (coflow) {
      int num_flow = (int) coflow->GetFlows()->size();
      JobDesc* newJobPtr = new JobDesc(jobid, jobOffArrivalTime,
                                       map, red, num_flow, coflow);
      // add entry into the map
      m_coflow2job.insert(std::make_pair(coflow, newJobPtr));
      result.push_back(newJobPtr);
    }
  } // read next line until a solid coflow is found.
  return result;
}

