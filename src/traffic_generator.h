//
//  traffic_genrator.h
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/21/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#ifndef TRAFFIC_GENERATOR_H
#define TRAFFIC_GENERATOR_H

#include <assert.h>
#include <fstream>
#include <map>
#include <set>
#include <vector>

#include "global.h"
#include "db_logger.h"

using namespace std;

class Flow;
class Coflow;
class JobDesc;
class Simulator;
class TrafficGenTimeLine;

class TrafficGen {
 public:
  TrafficGen();
  virtual ~TrafficGen();

  void InstallSimulator(Simulator *simPtr) { m_simPtr = simPtr; }
  /* called by simulator */
  virtual void NotifySimStart() = 0;
  virtual void TrafficGenAlarmPortal(double time) = 0;

  /* called by simulator */
  virtual void NotifyTrafficFinish(double alarmTime,
                                   vector<Coflow *> *cfpVp,
                                   vector<Flow *> *fpVp) = 0;
  virtual void NotifySimEnd() = 0;

 protected:
  virtual void PlaceTasks(int coflow_id, int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) = 0;
  Simulator *m_simPtr;
  TrafficGenTimeLine *m_myTimeLine;

  DbLogger *db_logger_; // Not owned.

  double m_currentTime;

  // internal auditing
  double m_totalCCT;
  double m_totalFCT;
  map<int, double> jobid_to_CCT_;
  int m_totalCoflowNum;
  int m_total_accepted_coflow_num;
  int m_total_met_deadline_num;
  ifstream m_jobTraceFile;

  ofstream m_cctAuditFile;
  ofstream m_fctAuditFile;

  void UpdateAlarm(); // update alarm on simulator

  // Use for testing or debugging. If TEST_ONLY_SAVE_COFLOW_AFTER_FINISH is
  // true, then we do NOT delete coflow when it is done but save the pointer in
  // coflows_saved_. coflows_saved_ will be deleted upon destruction of this
  // traffic generator. Coflows are ordered by their completion time.
  vector<Coflow *> coflows_saved_;

  friend class Simulator;
};

////////////////////////////////////////////////////
///////////// Code for FB Trace Replay   ///////////
////////////////////////////////////////////////////
class TGTraceFB : public TrafficGen {
 public:
  TGTraceFB(DbLogger *db_logger);
  virtual ~TGTraceFB();

  /* called by simulator */
  void NotifySimStart();
  void TrafficGenAlarmPortal(double time);

  /* called by simulator */
  virtual void NotifyTrafficFinish(double alarm_time,
                                   vector<Coflow *> *coflows_ptr,
                                   vector<Flow *> *flows_ptr);
  void NotifySimEnd();

 protected:
  vector<JobDesc *> m_runningJob; // allow Neat to access.
 private:
  vector<JobDesc *> m_finishJob;
  vector<JobDesc *> m_readyJob;

  virtual void DoSubmitJob();

  // Random seed bookeeping - one seed for each coflow.
  void InitSeedForCoflows(int seed_for_seed);
  vector<unsigned int> m_seed_for_coflow;

  // Obtain reducer input bytes and the reducer assigned machines from
  // reducer_trace. reducer_trace is formated as
  // r1_location : r1_input; r2_location : r2_input; ...
  // input are rounded to the closest MB.
  // remember the locations and input bytes *in order* for each reducer.
  void GetReducerInfoFromString(int num_reducer_wanted,
                                const string &reducer_trace,
                                vector<int> *original_locations,
                                vector<long> *reducer_input_bytes);
  vector<int> GetMapperOriginalLocFromString(int num_mapper_wanted,
                                             const string &mapper_trace);
  // TODO: remove unused.
  map<pair<int, int>, long>
  GetFlowSizeWithExactSize(int numMap, int numRed,
                           const vector<long> &redInput);
  // TODO: remove unused.
  map<pair<int, int>, long>
  GetFlowSizeWithEqualSizeToSameReducer(int numMap, int numRed,
                                        const vector<long> &redInput);
  // flow sizes will be +/- 1MB * perturb_perc%.
  // perturb_perc is a percentage, i.e. when perturb_perc = 10, then
  //    flow sizes will be +/- 0.1 MB.
  // only allow flow >= 1MB.
  map<pair<int, int>, long>
  GetFlowSizeWithPerturb(int numMap, int numRed,
                         const vector<long> &redInput,
                         int perturb_perc, unsigned int rand_seed);

  // By default, place mappers and reducers on the original locations specified.
  virtual void PlaceTasks(int coflow_id, int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) {
    assert(num_map == mapper_original_locations.size());
    assert(num_red == reducer_original_locations.size());
    *mapper_locations = mapper_original_locations;
    *reducer_locations = reducer_original_locations;
  }

 protected:
  virtual vector<JobDesc *> ReadJobs();
  Coflow *CreateCoflowPtrFromString(double time, int coflow_id,
                                    int num_map, int num_red,
                                    string cfInfo,
                                    bool do_perturb, bool avg_size);
  void GetNodeReqTrafficMB(int num_map, int num_red,
                           const map<pair<int, int>, long> &mr_flow_bytes,
                           vector<double> *mapper_traffic_req_MB,
                           vector<double> *reducer_traffic_req_MB);
  Coflow *CreateCoflow(int coflow_id, double arrival_time,
                       int num_map, int num_red,
                       const map<pair<int, int>, long> &mr_flow_bytes,
                       const vector<int> &mapper_locations,
                       const vector<int> &reducer_locations);

  void ScheduleToAddJobs(vector<JobDesc *> &jobs);
  void KickStartReadyJobsAndNotifyScheduler();
  map<Coflow *, JobDesc *> m_coflow2job;

  int GetSeedForCoflow(int coflow_id);

  friend class TrafficGeneratorTest;
  friend class TrafficAnalyzerTest;
};

class JobDesc {
 public:
  JobDesc(int iid,
          double offTime,
          int numSrc,
          int numDst,
          int numFlow,
          Coflow *cfp) : m_id(iid),
                         m_offArrivalTime(offTime),
                         m_numSrc(numSrc),
                         m_numDst(numDst),
                         m_numFlow(numFlow),
                         m_coflow(cfp) {}
  ~JobDesc() {}

  int m_id; // task id
  double m_offArrivalTime; // offset arrival time
  int m_numSrc;
  int m_numDst;
  int m_numFlow;
  Coflow *m_coflow; // one coflow per task, not owned.
};


///////////////////////////////////////////////////////////////////
///////////// Code for back-to-back coflow              ///////////
///////////// Replay FB trace but ignore real arrival time  ///////
///////////// so that one coflow is served at a time.     /////////
///////////////////////////////////////////////////////////////////
class TGFBOnebyOne : public TGTraceFB {
 public:
  TGFBOnebyOne(DbLogger *db_logger);
  virtual ~TGFBOnebyOne();
 private:
  // only submit the job.
  // not reading next job.
  virtual void DoSubmitJob() override;
  // add a coflow when one finishes.
  virtual void NotifyTrafficFinish(double alarmTime,
                                   vector<Coflow *> *cfpVp,
                                   vector<Flow *> *fpVp) override;

  // return a coflow/job at a time until the end of the trace.
  virtual vector<JobDesc *> ReadJobs() override;

  void LogComptimeStat(vector<Coflow *> *cfpVp);
  ofstream m_comptimeAuditFile;
  bool m_comptime_audit_title_line;

 protected:
  // used by child class TGWindowOnebyOne
  double m_last_coflow_finish_time;
};


class TrafficFactory {
 public:
  static TrafficGen *Get(string traffic_generator_name, DbLogger *db_logger) {
    if (traffic_generator_name.substr(0, 6) == "fbplay") {
      return new TGTraceFB(db_logger);
    } else if (traffic_generator_name == "fb1by1") {
      return new TGFBOnebyOne(db_logger);
      // comp time stat only available under "1by1".
      LOG_COMP_STAT = true;
      // Should be false unless hacking to obtain quick results.
      SPEEDUP_1BY1_TRAFFIC_IN_SCHEDULER = true;
    }
    return nullptr;
  }
};
#endif /*TRAFFIC_GENERATOR_H*/
