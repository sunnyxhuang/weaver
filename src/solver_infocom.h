//
// Created by Xin Sunny Huang on 10/5/17.
//

#ifndef XIMULATOR_SOLVER_INFOCOM_H
#define XIMULATOR_SOLVER_INFOCOM_H

#include "gurobi_c++.h"
#include "scheduler.h"

#include <map>
#include <set>
#include <vector>

class Coflow;
class Flow;
class Scheduler;

using namespace std;

class SolverInfocom {
 public:
  SolverInfocom(vector<Scheduler*>& schedulers, int debug_level = 0);
  virtual ~SolverInfocom() {}

  void ComputeRouteAndRate(vector<Coflow*>& coflows, map<long, long>* rates);

  // two alternatives to algorithm 1 line 2-17 in infocom'15 paper
  void ComputeRouteAndRatePaper(
      vector<Coflow*>& coflows, map<long, long>* rates,
      map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
      map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps);
  void ComputeRouteAndRateSunny(
      vector<Coflow*>& coflows, map<long, long>* rates,
      map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
      map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps);

  // TODO: make the following functions private or protected if needed.
  double MinCCT(
      Coflow* coflow,
      const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
      const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps);
  bool FindRoute(
      Coflow* coflow,
      const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
      const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps);

 private:
  vector<Scheduler*> schedulers_;
  const int debug_level_;

  // Distributed residual bandwidth according to reserved_bps records, mark up
  // flow rates in rates, also update the reserved_bps records.
  void DistributeBandwidth(
      vector<Coflow*>& coflows, map<long, long>* rates,
      map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
      map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps);
  static double FindCCTGivenRoute(
      Coflow* coflow,
      const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
      const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps,
      int debug_level);
  static bool IsValidRates(vector<Coflow*>& coflows,
                           const map<long, long>& rates,
                           bool exit_if_invalid,
                           int debug_level);

  struct AscendingFlowOnCoflowIdSrcDst {
    bool operator()(Flow* l, Flow* r) const;
  };

  struct AscendingSchedulerPort {
    bool operator()(const std::pair<Scheduler*, int>& l,
                    const std::pair<Scheduler*, int>& r) const;
  };
};

#endif //XIMULATOR_SOLVER_INFOCOM_H
