//
// Created by Xin Sunny Huang on 10/5/17.
//

#include <algorithm> // stable_sort

#include "global.h"
#include "solver_infocom.h"

SolverInfocom::SolverInfocom(vector<Scheduler*>& schedulers, int debug_level)
    : schedulers_(schedulers), debug_level_(debug_level) {}

void SolverInfocom::ComputeRouteAndRatePaper(
    vector<Coflow*>& coflows, map<long, long>* rates,
    map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
    map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps) {

  set<Coflow*> unscheduled_coflows(coflows.begin(), coflows.end());
  while (!unscheduled_coflows.empty()) {
    Coflow* coflow_min = nullptr;
    double cct_min = -1;
    for (Coflow* coflow: unscheduled_coflows) {
      double cct = MinCCT(
          coflow, *scheduler_src_reserved_bps, *scheduler_dst_reserved_bps);
      coflow->infocom_base_cct_ = cct;
      if (debug_level_ >= 1) {
        cout << coflow->GetName() << " solver_cct = " << cct << endl;
      }
      if ((cct > 0 && cct < cct_min) || cct_min < 0) {
        // we pick the coflow with smallest cct to schedule
        cct_min = cct;
        coflow_min = coflow;
      }
    }
    // we now schedule the smallest coflow
    if (cct_min < 0) {
      // each coflow has no feasible rate.
      break; // while (!unscheduled_coflows.empty())
    }
    // cct_min should be valid if when reach here.
    assert(coflow_min);
    assert(coflow_min->has_route_);
    for (Flow* flow: *coflow_min->GetFlows()) {
      if (!flow->HasDemand()) {
        continue;
      }
      long rate = long(flow->GetBitsLeft() / cct_min);
      if (rate <= 2) {
        // ignore rates that is too small
        continue; // with next flow
      }
      if (debug_level_ >= 2) {
        cout << "[SolverInfocom::ComputeRouteAndRatePaper]"
             << coflow_min->GetName() << " " << flow->toString()
             << " rate " << rate << endl;
      }
      rates->operator[](flow->GetFlowId()) = rate;
      Scheduler* scheduler = flow->assigned_scheduler_;
      int src = flow->GetSrc(), dst = flow->GetDest();
      scheduler_src_reserved_bps->operator[](std::make_pair(scheduler, src)) +=
          rate;
      scheduler_dst_reserved_bps->operator[](std::make_pair(scheduler, dst)) +=
          rate;
    }
    if (debug_level_ >= 1) {
      cout << "[SolverInfocom::ComputeRouteAndRatePaper] Scheduled "
           << coflow_min->GetName() << " with cct = " << cct_min << endl;
    }
    unscheduled_coflows.erase(coflow_min);
  }
}
void SolverInfocom::ComputeRouteAndRateSunny(
    vector<Coflow*>& coflows, map<long, long>* rates,
    map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
    map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps) {
  // calculate cct as if each coflow is the only one in the network.
  for (Coflow* coflow: coflows) {
    map<pair<Scheduler*, int>, long> no_src_reserved_bps, no_dst_reserved_bps;
    double cct = MinCCT(coflow, no_src_reserved_bps, no_dst_reserved_bps);
    coflow->infocom_base_cct_ = cct;
  }
  // sort all coflows based on their solo cct
  vector<Coflow*> sorted_coflows = coflows;
  std::stable_sort(sorted_coflows.begin(), sorted_coflows.end(),
                   [](Coflow* l, Coflow* r) {
                     return l->infocom_base_cct_ < r->infocom_base_cct_;
                   });
  // schedule coflows by solo-cct-first
  for (Coflow* coflow : sorted_coflows) {
    if (debug_level_ >= 2) {
      cout << "Scheduling " << coflow->GetName()
           << " with infocom_cct " << coflow->infocom_base_cct_ << endl;
    }
    double cct = MinCCT(
        coflow, *scheduler_src_reserved_bps, *scheduler_dst_reserved_bps);
    coflow->infocom_base_cct_ = cct;
    for (Flow* flow: *coflow->GetFlows()) {
      if (!flow->HasDemand()) {
        continue;
      }
      long rate = long(flow->GetBitsLeft() / cct);
      if (rate <= 0) {
        // this flow might take too long to finish
        continue;
      }
      rates->operator[](flow->GetFlowId()) = rate;
      Scheduler* scheduler = flow->assigned_scheduler_;
      int src = flow->GetSrc(), dst = flow->GetDest();
      scheduler_src_reserved_bps->operator[](std::make_pair(scheduler, src)) +=
          rate;
      scheduler_dst_reserved_bps->operator[](std::make_pair(scheduler, dst)) +=
          rate;
    }
    if (debug_level_ >= 2) {
      cout << "Scheduled " << coflow->GetName() << " with cct " << cct << endl;
    }
  }

}

void SolverInfocom::ComputeRouteAndRate(vector<Coflow*>& coflows,
                                        map<long, long>* rates) {

  map<pair<Scheduler*, int>, long> scheduler_src_reserved_bps;
  map<pair<Scheduler*, int>, long> scheduler_dst_reserved_bps;

  // ComputeRouteAndRateSunny v.s. ComputeRouteAndRatePaper
  ComputeRouteAndRatePaper(coflows, rates, &scheduler_src_reserved_bps,
                           &scheduler_dst_reserved_bps);
  // Work conservation
  DistributeBandwidth(coflows, rates,
                      &scheduler_src_reserved_bps,
                      &scheduler_dst_reserved_bps);

  IsValidRates(coflows, *rates, true/*exit_if_invalid*/,
               debug_level_);
}

double SolverInfocom::MinCCT(
    Coflow* coflow,
    const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
    const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps) {
  if (FindRoute(coflow,
                scheduler_src_reserved_bps, scheduler_dst_reserved_bps)) {
    return FindCCTGivenRoute(coflow,
                             scheduler_src_reserved_bps,
                             scheduler_dst_reserved_bps,
                             debug_level_);
  }
  return -1;
}

bool SolverInfocom::FindRoute(
    Coflow* coflow,
    const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
    const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps) {
  // clear routing info
  for (Flow* flow: *coflow->GetFlows()) {
    flow->assigned_scheduler_ = nullptr;
  }
  coflow->has_route_ = false;
  try {
    GRBEnv env = GRBEnv();

    GRBModel model = GRBModel(env);

    if (debug_level_ == 0) {
      model.getEnv().set(GRB_IntParam_OutputFlag, 0); // Gurobi output
    }

    // variable : alpha = 1/cct
    GRBVar alpha = model.addVar(0.0, GRB_INFINITY, 0.0, GRB_CONTINUOUS, "a");

    // containers for (m_j_k), i.e. routing
    map<pair<Scheduler*, int>, vector<pair<GRBVar, double>>,
        AscendingSchedulerPort> scheduler_src_to_vars_m_v;
    map<pair<Scheduler*, int>, vector<pair<GRBVar, double>>,
        AscendingSchedulerPort> scheduler_dst_to_vars_m_v;
    map<Flow*, map<Scheduler*, GRBVar>,
        AscendingFlowOnCoflowIdSrcDst> flow_to_scheduler_var_m;

    for (Flow* flow: *coflow->GetFlows()) {
      if (!flow->HasDemand()) {
        continue;
      }
      for (Scheduler* scheduler  : schedulers_) {
        GRBVar m = model.addVar(
            0.0, GRB_INFINITY, 0.0, GRB_CONTINUOUS,
            "m_" + to_string(flow->GetFlowId()) + "_" + scheduler->name_);
        // we use Gbits for variable v
        scheduler_src_to_vars_m_v[
            std::make_pair(scheduler, flow->GetSrc())].push_back(
            std::make_pair(m, flow->GetBitsLeft() / 1e9));
        scheduler_dst_to_vars_m_v[
            std::make_pair(scheduler, flow->GetDest())].push_back(
            std::make_pair(m, flow->GetBitsLeft() / 1e9));
        flow_to_scheduler_var_m[flow][scheduler] = m;
      }
    }

    // Integrate new variables
    model.update();

    // Set objective: maximize 1/cct, or minimize cct
    GRBLinExpr obj = alpha;
    model.setObjective(obj, GRB_MAXIMIZE);

    // Add constraint: inbound cap
    for (const auto& tuple: scheduler_src_to_vars_m_v) {
      Scheduler* scheduler = tuple.first.first;
      int src = tuple.first.second;
      GRBLinExpr constraint;
      for (const auto& m_v_pair:tuple.second) {
        constraint += m_v_pair.first * m_v_pair.second;
      }
      long residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          scheduler_src_reserved_bps, std::make_pair(scheduler, src), 0L);
      model.addConstr(constraint <= residual_bps / 1e9,
                      "Csrc_" + scheduler->name_ + "_" + to_string(src));
      if (debug_level_ >= 3) {
        cout << "Added constraints " << scheduler->name_ << " src " << src
             << " with residual_bps = " << residual_bps << endl;
      }
    }

    // Add constraint: outbound cap
    for (const auto& tuple: scheduler_dst_to_vars_m_v) {
      Scheduler* scheduler = tuple.first.first;
      int dst = tuple.first.second;
      GRBLinExpr constraint;
      for (const auto& m_v_pair:tuple.second) {
        constraint += m_v_pair.first * m_v_pair.second;
      }
      long residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          scheduler_dst_reserved_bps, std::make_pair(scheduler, dst), 0L);
      model.addConstr(constraint <= residual_bps / 1e9,
                      "Cdst_" + scheduler->name_ + "_" + to_string(dst));
      if (debug_level_ >= 3) {
        cout << "Added constraints " << scheduler->name_ << " dst " << dst
             << " with residual_bps = " << residual_bps << endl;
      }
    }

    // Add constraint: one path per flow
    for (const auto& flow_vars : flow_to_scheduler_var_m) {
      Flow* flow = flow_vars.first;
      GRBLinExpr constraint;
      for (const auto& scheduler_var_pair : flow_vars.second) {
        constraint += scheduler_var_pair.second;
      }
      model.addConstr(constraint == alpha,
                      "Cflow_" + to_string(flow->GetFlowId()));
    }

    // Optimize model
    model.optimize();

    if (model.get(GRB_IntAttr_Status) == GRB_OPTIMAL) {
      for (const auto& flow_map_pair : flow_to_scheduler_var_m) {
        Flow* flow = flow_map_pair.first;
        Scheduler* best_scheduler = nullptr;
        double best_m = -1;
        if (debug_level_ >= 4) {
          cout << flow->toString() << endl; // debug
        }
        for (const auto& scheduler_var_m:flow_map_pair.second) {
          Scheduler* scheduler = scheduler_var_m.first;
          double scheduler_m = scheduler_var_m.second.get(GRB_DoubleAttr_X);
          // debug
          if (debug_level_ >= 4) {
            cout << "     " << scheduler->name_ << ", m=" << scheduler_m << ", "
                 << scheduler_var_m.second.get(GRB_StringAttr_VarName) << endl;
          }
          if (best_m < scheduler_m || best_m < 0) {
            best_scheduler = scheduler;
            best_m = scheduler_m;
          }
        } // for schedulers
        assert(best_scheduler);
        flow->assigned_scheduler_ = best_scheduler;
        if (debug_level_ >= 4) {
          cout << coflow->GetName() << " " << flow->toString()
               << " assigned to " << best_scheduler->name_ << endl;
        }
      }
      coflow->has_route_ = true;
      if (debug_level_ >= 2) {
        cout << coflow->GetName() << " solver_cct = "
             << 1 / alpha.get(GRB_DoubleAttr_X) << " (could be infeasible)\n";
      }
      return true;
    } else {
      cerr << " NOT feasible\n";
      return false;
    }

  } catch (GRBException e) {
    cerr << "Gurobi Error: " << e.getMessage() << endl;
  } catch (...) {
    cerr << "Exception during optimization" << endl;
  }
  cout << "By default: NOT feasible\n";
  return false;

  return 0;
}

void SolverInfocom::DistributeBandwidth(
    vector<Coflow*>& coflows, map<long, long>* rates,
    map<pair<Scheduler*, int>, long>* scheduler_src_reserved_bps,
    map<pair<Scheduler*, int>, long>* scheduler_dst_reserved_bps) {
  vector<Coflow*> sorted_coflows = coflows;
  std::stable_sort(sorted_coflows.begin(), sorted_coflows.end(),
                   [](Coflow* l, Coflow* r) {
                     return l->infocom_base_cct_ < 0 || r->infocom_base_cct_ < 0
                            ? l->infocom_base_cct_ < r->infocom_base_cct_ :
                            l->infocom_base_cct_ > r->infocom_base_cct_;
                   });
  for (Coflow* coflow: sorted_coflows) {
    if (debug_level_ >= 2) {
      cout << "WC " << coflow->GetName() << " infocom_cct ="
           << coflow->infocom_base_cct_ << endl;
    }
    vector<Flow*> sorted_flows = *coflow->GetFlows();
    std::stable_sort(sorted_flows.begin(), sorted_flows.end(),
                     [](Flow* l, Flow* r) {
                       return l->GetBitsLeft() > r->GetBitsLeft();
                     });
    for (Flow* flow: sorted_flows) {
      if (!flow->HasDemand()) {
        continue; // with next flow
      }
      if (debug_level_ >= 5) {
        cout << "WC " << flow->toString() << " " << endl;
      }
      Scheduler* scheduler = flow->assigned_scheduler_;
      int src = flow->GetSrc(), dst = flow->GetDest();
      long src_residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          *scheduler_src_reserved_bps, std::make_pair(scheduler, src), 0L);
      long dst_residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          *scheduler_dst_reserved_bps, std::make_pair(scheduler, dst), 0L);
      long residual_bps = min(src_residual_bps, dst_residual_bps);
      if (residual_bps <= 2) {
        // ignore increment that is too small
        continue; // with next flow
      }
      // increase the rate!
      rates->operator[](flow->GetFlowId()) += residual_bps;
      scheduler_src_reserved_bps->operator[](std::make_pair(scheduler, src))
          += residual_bps;
      scheduler_dst_reserved_bps->operator[](std::make_pair(scheduler, dst))
          += residual_bps;
      if (debug_level_ >= 4) {
        cout << "WC Added rate " << residual_bps << " for "
             << coflow->GetName() << " " << flow->toString() << " new rate = "
             << FindWithDef(*rates, flow->GetFlowId(), 0L) << endl;
      }
    }// for flow
  }// for coflow
}

// static
double SolverInfocom::FindCCTGivenRoute(
    Coflow* coflow,
    const map<pair<Scheduler*, int>, long>& scheduler_src_reserved_bps,
    const map<pair<Scheduler*, int>, long>& scheduler_dst_reserved_bps,
    int debug_level) {

  assert(coflow->has_route_);

  map<pair<Scheduler*, int>, long, AscendingSchedulerPort>
      scheduler_src_to_bits;
  map<pair<Scheduler*, int>, long, AscendingSchedulerPort>
      scheduler_dst_to_bits;
  for (Flow* flow :*coflow->GetFlows()) {
    if (!flow->HasDemand()) {
      continue;
    }
    Scheduler* scheduler = flow->assigned_scheduler_;
    if (!scheduler) {
      cerr << flow->toString() << " has no scheduler!!!\n";
    }
    assert(scheduler); // bug?
    scheduler_src_to_bits[std::make_pair(scheduler, flow->GetSrc())] +=
        flow->GetBitsLeft();
    scheduler_dst_to_bits[std::make_pair(scheduler, flow->GetDest())] +=
        flow->GetBitsLeft();
  }

  try {

    GRBEnv env = GRBEnv();

    GRBModel model = GRBModel(env);
    model.getEnv().set(GRB_IntParam_OutputFlag, 0); // disable output

    // objective
    GRBVar cct = model.addVar(0.0, GRB_INFINITY, 0.0, GRB_CONTINUOUS, "cct");

    // Integrate new variables
    model.update();

    // Set objective: minimize cct
    GRBLinExpr obj = cct;
    model.setObjective(obj, GRB_MINIMIZE);

    // Add constraint: inbound cap
    for (const auto& tuple: scheduler_src_to_bits) {
      Scheduler* scheduler = tuple.first.first;
      int src = tuple.first.second;
      long src_bits = tuple.second;
      long residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          scheduler_src_reserved_bps, std::make_pair(scheduler, src), 0L);
      model.addConstr(src_bits / 1e9 <= cct * residual_bps / 1e9,
                      "Csrc_" + scheduler->name_ + "_" + to_string(src));
      // debug
      // cout << "[FindCCTGivenRoute]: " << scheduler->name_ << " src " << src
      //     << " residual " << residual_bps << " src_bits " << src_bits << endl;
    }

    // Add constraint: outbound cap
    for (const auto& tuple: scheduler_dst_to_bits) {
      Scheduler* scheduler = tuple.first.first;
      int dst = tuple.first.second;
      long dst_bits = tuple.second;
      long residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_ - FindWithDef(
          scheduler_dst_reserved_bps, std::make_pair(scheduler, dst), 0L);
      model.addConstr(dst_bits / 1e9 <= cct * residual_bps / 1e9,
                      "Cdst_" + scheduler->name_ + "_" + to_string(dst));
      // debug
      // cout << "[FindCCTGivenRoute]: " << scheduler->name_ << " dst " << dst
      //     << " residual " << residual_bps << " dst_bits " << dst_bits << endl;
    }

    // Optimize model
    model.optimize();

    if (model.get(GRB_IntAttr_Status) == GRB_OPTIMAL) {
      double solver_cct_given_route = cct.get(GRB_DoubleAttr_X);
      if (debug_level >= 1) {
        cout << coflow->GetName() << " solver_cct_given_route = "
             << solver_cct_given_route << ", ratio over lowerbound = "
             << solver_cct_given_route * ELEC_BPS
                 / coflow->GetMaxPortLoadInBits() << endl;
      }
      return solver_cct_given_route;
    } else {
      if (debug_level >= 1) {
        cout << "[FindCCTGivenRoute]: NO feasible cct for "
             << coflow->GetName() << endl;
        // exit(-1);
      }
      return -1;
    }

  } catch (GRBException e) {
    cerr << "Gurobi Error: " << e.getMessage() << endl;
  } catch (...) {
    cerr << "Exception during optimization" << endl;
  }
  cout << "[FindCCTGivenRoute]: By default NOT feasible\n";
  return -1;
}

//static
bool SolverInfocom::IsValidRates(vector<Coflow*>& coflows,
                                 const map<long, long>& rates,
                                 bool exit_if_invalid,
                                 int debug_level) {
  if (coflows.empty()) return true;
  map<pair<Scheduler*, int>, long> validate_scheduler_src_bps;
  map<pair<Scheduler*, int>, long> validate_scheduler_dst_bps;
  for (Coflow* coflow: coflows) {
    for (Flow* flow: *coflow->GetFlows()) {
      if (!ContainsKey(rates, flow->GetFlowId())) {
        continue;
      }
      long rate = FindWithDef(rates, flow->GetFlowId(), 0L);
      assert(rate > 0);
      validate_scheduler_src_bps[std::make_pair(flow->assigned_scheduler_,
                                                flow->GetSrc())] += rate;
      validate_scheduler_dst_bps[std::make_pair(flow->assigned_scheduler_,
                                                flow->GetDest())] += rate;
    }// for flow
  } // for coflow

  long too_much_wasted_bps = 1000L;
  // validate inbound cap
  for (const auto& tuple: validate_scheduler_src_bps) {
    Scheduler* scheduler = tuple.first.first;
    int src = tuple.first.second;
    long reserved_bps = tuple.second;
    // debug
    // cout << scheduler->name_ << " src " << src << " reserved bps "
    //     << reserved_bps << endl;
    if (reserved_bps > scheduler->SCHEDULER_LINK_RATE_BPS_) {
      cerr << "Error: scheduler " << scheduler->name_ << " src " << src << " "
           << "Invalid reserved bps " << reserved_bps << " > "
           << scheduler->SCHEDULER_LINK_RATE_BPS_ << endl;
      if (exit_if_invalid) {
        exit(-1);
      } else {
        return false;
      }
    }
  }

  // validate outbound cap
  for (const auto& tuple: validate_scheduler_dst_bps) {
    Scheduler* scheduler = tuple.first.first;
    int dst = tuple.first.second;
    long reserved_bps = tuple.second;
    // debug
    // cout << scheduler->name_ << " dst " << dst << " reserved bps "
    //      << reserved_bps << endl;
    if (reserved_bps > scheduler->SCHEDULER_LINK_RATE_BPS_) {
      cerr << "Error: scheduler " << scheduler->name_ << " dst " << dst << " "
           << "Invalid reserved bps " << reserved_bps << " > "
           << scheduler->SCHEDULER_LINK_RATE_BPS_ << endl;
      if (exit_if_invalid) {
        exit(-1);
      } else {
        return false;
      }
    }
  }


  // too much wasted bandwidth ?
  long waste_threshold_bps = 100L;
  map<pair<int, int>, long> agg_demand;
  for (Coflow* coflow: coflows) {
    for (Flow* flow:*coflow->GetFlows()) {
      if (!flow->HasDemand() || !flow->assigned_scheduler_) {
        continue;
      }
      Scheduler* scheduler = flow->assigned_scheduler_;
      int src = flow->GetSrc(), dst = flow->GetDest();
      long src_residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_
          - FindWithDef(validate_scheduler_src_bps,
                        std::make_pair(scheduler, src), 0L/*default_val*/);
      long dst_residual_bps = scheduler->SCHEDULER_LINK_RATE_BPS_
          - FindWithDef(validate_scheduler_dst_bps,
                        std::make_pair(scheduler, dst), 0L/*default_val*/);
      long wasted_bps = min(src_residual_bps, dst_residual_bps);
      if (wasted_bps >= waste_threshold_bps && debug_level >= 1) {
        cout << "Warning: wasted bps? " << coflow->GetName() << " "
             << flow->toString() << " scheduler " << scheduler->name_ << " "
             << "wasted bps = " << wasted_bps << endl;
      }
    }
  }

  return true;
}

bool SolverInfocom::AscendingFlowOnCoflowIdSrcDst::operator()(Flow* l,
                                                              Flow* r) const {
  if (l->GetParentCoflow()->GetJobId() != r->GetParentCoflow()->GetJobId()) {
    return l->GetParentCoflow()->GetJobId() < r->GetParentCoflow()->GetJobId();
  }
  // same job id
  if (l->GetSrc() != r->GetSrc()) {
    return l->GetSrc() < r->GetSrc();
  }
  // same src
  return l->GetDest() < r->GetDest();
}

bool SolverInfocom::AscendingSchedulerPort::operator()(
    const std::pair<Scheduler*, int>& l,
    const std::pair<Scheduler*, int>& r) const {
  if (l.first->name_ != r.first->name_) {
    return l.first->name_ < r.first->name_;
  }
  // same scheduler name
  if (l.first->SCHEDULER_LINK_RATE_BPS_ != r.first->SCHEDULER_LINK_RATE_BPS_) {
    return l.first->name_ < r.first->name_;
  }
  // same scheduler link rate
  return l.second < r.second;
}
