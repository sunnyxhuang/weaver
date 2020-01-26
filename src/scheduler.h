//
//  scheduler.h
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/21/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <algorithm> // std::replace
#include <fstream>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <sstream>
#include <vector>

#include "coflow.h"
#include "db_logger.h"

extern long DEFAULT_LINK_RATE_BPS;
extern long ELEC_BPS;

using namespace std;

class Flow;
class Coflow;
class CompTimeBreakdown;
class Simulator;
class SchedulerTimeLine;

class Scheduler {
 public:
  Scheduler(long scheduler_link_rate = DEFAULT_LINK_RATE_BPS);
  virtual ~Scheduler();
  virtual void InstallSimulator(Simulator* simPtr) { m_simPtr = simPtr; }
  virtual void SchedulerAlarmPortal(double) = 0;
  void NotifySimEnd();

  virtual void NotifyAddCoflows(double, vector<Coflow*>*);
  virtual void NotifyAddFlows(double);

  std::string name_;
  const long SCHEDULER_LINK_RATE_BPS_;

  static const double INVALID_RATE_;

 protected:
  static int instance_count_;
  Simulator* m_simPtr;
  double m_currentTime;
  SchedulerTimeLine* m_myTimeLine;
  vector<Coflow*> m_coflowPtrVector;

  map<long, long> m_nextElecRate;          /* maps flow ID to next elec rate */
  map<long, long> m_nextOptcRate;          /* maps flow ID to next optc rate */

  map<int, long> m_sBpsFree_before_workConserv;
  map<int, long> m_rBpsFree_before_workConserv;

  // returns true if has flow finish during transfer.
  virtual bool Transmit(double startTime,
                        double endTime,
                        bool basic,
                        bool local,
                        bool salvage);
  virtual void ScheduleToNotifyTrafficFinish(double end_time,
                                             vector<Coflow*>& coflows_done,
                                             vector<Flow*>& flows_done);
  virtual void CoflowFinishCallBack(double finishtime) = 0;
  virtual void FlowFinishCallBack(double finishTime) = 0;

  bool ValidateLastTxMeetConstraints(long port_bound_bits,
                                     const map<int, long>& src_tx_bits,
                                     const map<int, long>& dst_tx_bits,
                                     const map<int, int>& src_flow_num,
                                     const map<int, int>& dst_flow_num);

  void SetFlowRate();

  virtual void UpdateAlarm();
  void UpdateRescheduleEvent(double reScheduleTime);
  double UpdateFlowFinishEvent(double baseTime);

  double SecureFinishTime(long bits, long rate);
  double CalcTime2FirstFlowEnd();
  void Print(void);
 private:

  friend class SchedulerHybrid;
  friend class SchedulerWeaver;
};

class SchedulerHybrid : public Scheduler {
 public:
  SchedulerHybrid(vector<Scheduler*>& schedulers);
  virtual ~SchedulerHybrid() {}
  virtual void SchedulerAlarmPortal(double alarm_time);
  virtual void InstallSimulator(Simulator* simulator);

  virtual void CoflowFinishCallBack(double finish_time);
  virtual void FlowFinishCallBack(double finish_time);
 protected:
  virtual bool ShouldGoBackup(Coflow* coflow_to_go,
                              long main_link_rate_bps,
                              long backup_link_rate_bps);
  // We assume 2 schedulers: main scheduler at the front, and backup scheduler
  // at the back.
  vector<unique_ptr<Scheduler>> schedulers_;

  virtual void CoflowArrive();

  // Require: main_link_rate_bps > 0 && backup_link_rate_bps > 0
  virtual void AssignCoflowsToSchedulers(
      const vector<Coflow*>& coflows_to_assign,
      long main_link_rate_bps, long backup_link_rate_bps,
      vector<Coflow*>* coflows_to_main, vector<Coflow*>* coflows_to_backup);
};

class SchedulerSplit : public SchedulerHybrid {
 public:
  SchedulerSplit(vector<Scheduler*>& schedulers)
      : SchedulerHybrid(schedulers) {}
  virtual ~SchedulerSplit() {}
 protected:
  virtual void SplitCoflow(Coflow* coflow_to_split,
                           long main_link_rate_bps,
                           long backup_link_rate_bps,
                           vector<Flow*>* flows_to_main,
                           vector<Flow*>* flows_to_backup);
  virtual void AssignCoflowsToSchedulers(
      const vector<Coflow*>& coflows_to_assign,
      long main_link_rate_bps, long backup_link_rate_bps,
      vector<Coflow*>* coflows_to_main, vector<Coflow*>* coflows_to_backup);
  ChildCoflow* CreateChildCoflowFromSubFlows(
      Coflow* parent_coflow, const vector<Flow*>& flows_in_coflow);
};

// K weaver heterogeneous switches
class SchedulerWeaver : public SchedulerSplit {
 public:
  typedef enum enum_Flow_Order_Mode {
    FLOW_ORDER_BEST,
    FLOW_SIZE_LARGE_FIRST, // default
    FLOW_SIZE_SMALL_FIRST,
    FLOW_SRC_DST_IDX_SMALL_FIRST,
    FLOW_ORDER_RANDOM,
    FLOW_ORDER_NONE
  } FlowOrderMode;

  typedef enum enum_Non_Critical_Mode {
    NON_CRITICAL_MIN_BN,
    NON_CRITICAL_RATIO_LB, // default
    NON_CRITICAL_RANDOM,
  } NonCriticalMode;

  SchedulerWeaver(
      vector<Scheduler*>& schedulers,
      FlowOrderMode flow_order_mode = FlowOrderMode::FLOW_SIZE_LARGE_FIRST,
      NonCriticalMode non_critical_mode = NonCriticalMode::NON_CRITICAL_RATIO_LB
  );
  virtual ~SchedulerWeaver() {}
  static SchedulerWeaver* Factory(
      std::string full_name, vector<Scheduler*>& children_schedulers);
 protected:
  virtual void CoflowArrive() override;
  // Required: each flow in each parent coflow must be assigned to a scheduler.
  virtual void AssignCoflowsToSchedulers(
      vector<Coflow*>& parent_coflows, vector<Scheduler*>& schedulers,
      map<Scheduler*, vector<Coflow*>*>* scheduler_to_children_coflows);
  Coflow* CreateChildCoflowFromParentFlows(
      Scheduler* scheduler, Coflow* parent,
      const vector<Flow*> flows_from_parent);

  template<typename Comparator>
  void SortAndRangeShuffle(Coflow* parent,
                           vector<Flow*>* sorted_flows,
                           Comparator compare);
  int debug_level_;

 private:
  FlowOrderMode flow_order_mode_;
  NonCriticalMode non_critical_mode_;

  friend class SolverTest_Weaver_ManyCoflow_Test;
  friend class SolverTest_Weaver_Example_Test;
  friend class SolverTest_Weaver_Incast_Test;
};

class SchedulerWeightedRandom : public SchedulerWeaver {
 public:
  SchedulerWeightedRandom(vector<Scheduler*>& schedulers, int run = 0)
      : SchedulerWeaver(schedulers), run_seed_(run), debug_level_(0) {
    cout << "run_seed_ = " << run_seed_ << endl;
  }
  virtual ~SchedulerWeightedRandom() {}
 protected:
  virtual void AssignCoflowsToSchedulers(
      vector<Coflow*>& parent_coflows, vector<Scheduler*>& schedulers,
      map<Scheduler*, vector<Coflow*>*>* scheduler_to_children_coflows);
 private:
  // TODO: run multiple runs with different run_seed_
  const int run_seed_;
  const int debug_level_;
};

class SchedulerVarys : public Scheduler {
 public:
  SchedulerVarys(long scheduler_link_rate_bps = ELEC_BPS)
      : Scheduler(scheduler_link_rate_bps) {
    name_ = to_string(instance_count_) + "varys"
        + to_string(int(SCHEDULER_LINK_RATE_BPS_ / 1e6)) + "Mbps";
  }
  virtual ~SchedulerVarys();
  void SchedulerAlarmPortal(double currentTime);
 protected:
  // override by Varys-Deadline.
  virtual void CoflowArrive();

 private:
  virtual void Schedule(void) = 0;
  // override by Aalo.
  virtual void AddCoflows(vector<Coflow*>* cfVecPtr);

  void ApplyNewSchedule(void);
  void AddFlows();
  void FlowArrive();
  void FlowFinishCallBack(double finishTime);
  void CoflowFinishCallBack(double finishTime);
};

class SchedulerInfocom : public SchedulerVarys {
 public:
  SchedulerInfocom(vector<Scheduler*> schedulers)
      : SchedulerVarys(), schedulers_(schedulers) {}
  virtual ~SchedulerInfocom() { for (Scheduler* s:schedulers_) delete s; }
 private:
  virtual void Schedule(void);
 protected:
  vector<Scheduler*> schedulers_;//owned.
};

class SchedulerAaloImpl : public SchedulerVarys {
 public:
  SchedulerAaloImpl(long scheduler_link_rate_bps = ELEC_BPS);
  virtual ~SchedulerAaloImpl() {}
 private:
  virtual void Schedule(void);

  virtual void AddCoflows(vector<Coflow*>* cfVecPtr);

  void RateControlAaloImpl(vector<Coflow*>& coflows,
                           vector<vector<int>>& coflow_id_queues,
                           map<long, long>& rates,
                           long LINK_RATE);
  void UpdateCoflowQueue(vector<Coflow*>& coflows,
                         vector<vector<int>>& last_coflow_id_queues,
                         map<int, Coflow*>& coflow_id_ptr_map);
  // used to maintain the stability of coflow queues.
  vector<vector<int>> m_coflow_jid_queues;
};


class SchedulerVarysImpl : public SchedulerVarys {
 public:
  SchedulerVarysImpl(long scheduler_link_rate_bps = ELEC_BPS)
      : SchedulerVarys(scheduler_link_rate_bps) {}
  virtual ~SchedulerVarysImpl() {}
 private:

  // either pointing to m_nextElecRate or m_nextOptcRate.
  // If poiting to m_nextOptcRate, this scheduler is for backup network and
  // should set thruOptics for each scheduled flow.
  // map<int, long> *control_rates_;

  virtual void Schedule(void);

  // Varys implemented in Github!
  // rate control based on selfish coflow
  // and the work conservation for each coflow's flow.
  // store flow rates in rates.
  void RateControlVarysImpl(vector<Coflow*>& coflows,
                            map<long, long>& rates,
                            long LINK_RATE);
  virtual void SortCoflows(vector<Coflow*>& coflows) {
    CalAlphaAndSortCoflowsInPlace(coflows);
  }

 protected:
  // as used by the deadline-mode varysImpl scheduler.
  // routine used RateControlVarysImpl.
  // work conservation in the Github implementation.
  void RateControlWorkConservationImpl(vector<Coflow*>& coflows,
                                       map<long, long>& rates,
                                       map<int, long>& sBpsFree,
                                       map<int, long>& rBpsFree,
                                       long LINK_RATE_BPS);

  friend class SolverTest_Varys_ManyCoflow_Test;
  friend class SolverTest_Weaver_ManyCoflow_Test;
};

class SchedulerFactory {
 public:
  static Scheduler* Get(const std::string& scheduler_name) {
    if (scheduler_name.substr(0, 6) == "weaver") {
      vector<Scheduler*>
          schedulers = GenerateChildrenSchedulers(scheduler_name);
      return SchedulerWeaver::Factory(scheduler_name, schedulers);
    } else if (scheduler_name.substr(0, 7) == "prandom") {
      vector<Scheduler*>
          schedulers = GenerateChildrenSchedulers(scheduler_name);
      // extract rand seed if any
      int run_seed = 0;
      std::string name_no_config =
          scheduler_name.substr(0, scheduler_name.find_first_of("_"));
      std::size_t const digit_idx = name_no_config.find_first_of("0123456789");
      if (digit_idx != std::string::npos) {
        run_seed = stoi(scheduler_name.substr(digit_idx));
      }
      return new SchedulerWeightedRandom(schedulers, run_seed);
    } else if (scheduler_name.substr(0, 7) == "infocom") {
      vector<Scheduler*>
          schedulers = GenerateChildrenSchedulers(scheduler_name);
      return new SchedulerInfocom(schedulers);
    } else if (scheduler_name == "varysImpl") {
      return new SchedulerVarysImpl();
    } else if (scheduler_name == "aaloImpl") {
      return new SchedulerAaloImpl();
    }
    cerr << "No scheduler found with name '" << scheduler_name << "'\n";
    return nullptr;
  }

  // used to customize individual scheduler's link rate
  static Scheduler* GetChild(const std::string& scheduler_name,
                             long link_rate_bps) {
    if (scheduler_name == "varys") {
      return new SchedulerVarysImpl(link_rate_bps);
    } else if (scheduler_name == "aalo") {
      return new SchedulerAaloImpl(link_rate_bps);
    }

    cerr << "No scheduler found with name '" << scheduler_name << "'\n";
    return nullptr;
  }

  // generate multiple children schdulers for the weaver scheduler modle.
  // two format of scheduler_name are accepted:
  // "weaver_ratioAscheduler_ratioBscheduler...",
  // or "weaver_Nnet_scheduler_ratioA_ratioB..."
  static vector<Scheduler*> GenerateChildrenSchedulers(
      const std::string& scheduler_name) {
    std::string canonical_name = scheduler_name;
    std::replace(canonical_name.begin(), canonical_name.end(), '_', ' ');
    std::istringstream line_stream(canonical_name);

    // assert and clear the leading string "weaver"
    string weaver_name;
    line_stream >> weaver_name;
    // assert(weaver_name == "weaver");

    int ratio_sum = 0;
    std::string default_scheduler_name;
    vector<Scheduler*> schedulers;
    while (!line_stream.eof()) {
      std::string name;
      int ratio;
      if (default_scheduler_name != "") {
        // using format "weaver_Nnet_scheduler_ratioA_ratioB..."
        line_stream >> ratio;
        name = default_scheduler_name;
      } else {
        // using format "weaver_ratioAscheduler_ratioBscheduler..."
        line_stream >> ratio >> name;
      }
      // cout << ratio << "  " << name << endl;
      if (name == "net") {
        line_stream >> default_scheduler_name;
        if (default_scheduler_name == "") {
          cout << "Must specify default scheduler name after Nnet config\n";
          exit(-1);
        }
        continue; // with ratios
      }

      Scheduler* new_scheduler =
          SchedulerFactory::GetChild(name, ELEC_BPS / 100 * ratio);
      if (new_scheduler) {
        schedulers.push_back(new_scheduler);
        ratio_sum += ratio;
      } else {
        cerr << "Illegal field in weaver scheduler name : " << name << endl;
        exit(-1);
      }
    }

    if (ratio_sum != 100) {
      cerr << "ratio sum (" << ratio_sum << ") should be 100 " << endl;
    }

    // debug
    cout << "Weaver scheduler's children: ";
    for (Scheduler* s:schedulers) cout << s->name_ << ", ";
    cout << endl;

    return schedulers;
  }
};

#endif /*SCHEDULER_H*/
