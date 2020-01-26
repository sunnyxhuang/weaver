//
// Created by Xin Sunny Huang on 9/25/17.
//

#include <assert.h>
#include <random>
#include <algorithm> // needed for find in stable_sort()

#include "events.h"
#include "global.h"
#include "scheduler.h"
#include "util.h"
#include "coflow.h"

SchedulerWeaver::SchedulerWeaver(vector<Scheduler *> &schedulers,
                                     FlowOrderMode flow_order_mode,
                                     NonCriticalMode non_critical_mode)
    : SchedulerSplit(schedulers), flow_order_mode_(flow_order_mode),
      non_critical_mode_(non_critical_mode), debug_level_(0) {} // DEBUG_LEVEL

// static
SchedulerWeaver *SchedulerWeaver::Factory(
    std::string full_name, vector<Scheduler *> &children_schedulers) {
  if (full_name.substr(0, 16) == "weaverSortedBest") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_ORDER_BEST);
  } else if (full_name.substr(0, 19) == "weaverSortedFlowDec") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SIZE_LARGE_FIRST);
  } else if (full_name.substr(0, 19) == "weaverSortedFlowInc") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SIZE_SMALL_FIRST);
  } else if (full_name.substr(0, 21) == "weaverSortedSrcDstIdx") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SRC_DST_IDX_SMALL_FIRST);
  } else if (full_name.substr(0, 18) == "weaverSortedRandom") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_ORDER_RANDOM);
  } else if (full_name.substr(0, 17) == "weaverNonCRatioLB") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SIZE_LARGE_FIRST,
                                 NonCriticalMode::NON_CRITICAL_RATIO_LB);
  } else if (full_name.substr(0, 16) == "weaverNonCRandom") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SIZE_LARGE_FIRST,
                                 NonCriticalMode::NON_CRITICAL_RANDOM);
  } else if (full_name.substr(0, 15) == "weaverNonCMinBn") {
    return new SchedulerWeaver(children_schedulers,
                                 FlowOrderMode::FLOW_SIZE_LARGE_FIRST,
                                 NonCriticalMode::NON_CRITICAL_MIN_BN);
  }
  return new SchedulerWeaver(children_schedulers);
}

void SchedulerWeaver::CoflowArrive() {

  EventCoflowArrive *event = (EventCoflowArrive *) m_myTimeLine->PeekNext();
  if (event->GetEventType() != COFLOW_ARRIVE) {
    cout << "[SchedulerWeaver::CoflowArrive] error: "
         << " the event type is not COFLOW_ARRIVE!" << endl;
    return;
  }

  // all parent coflows to split
  vector<Coflow *> parent_coflows = *event->m_cfpVp;

  // map from scheduler to children coflows to assign
  vector<Scheduler *> schedulers; // to be sorted by bandwidth
  map<Scheduler *, vector<Coflow *> *> scheduler_to_children_coflows;
  for (const unique_ptr<Scheduler> &scheduler : schedulers_) {
    if (scheduler->SCHEDULER_LINK_RATE_BPS_ > 0) {
      // only consider scheduler with valid bandwidth resource
      schedulers.push_back(scheduler.get());
      scheduler_to_children_coflows.insert(
          std::make_pair(scheduler.get(), new vector<Coflow *>()));
    }
  }
  std::stable_sort(schedulers.begin(), schedulers.end(),
                   [](Scheduler *l, Scheduler *r) {
                     return l->SCHEDULER_LINK_RATE_BPS_
                         >= r->SCHEDULER_LINK_RATE_BPS_;
                   });
  AssignCoflowsToSchedulers(parent_coflows, schedulers,
                            &scheduler_to_children_coflows);

  for (const auto &scheduler_coflows_pair: scheduler_to_children_coflows) {
    vector<Coflow *> *coflows_this_scheduler = scheduler_coflows_pair.second;
    if (coflows_this_scheduler->empty()) {
      // no child coflow assigned to this scheduler
      delete coflows_this_scheduler;
    } else {
      // invoke the scheduler to accept the assigned children coflows.
      Scheduler *target_scheduler = scheduler_coflows_pair.first;
      target_scheduler->m_myTimeLine->AddEvent
          (new EventCoflowArrive(m_currentTime, coflows_this_scheduler));
      target_scheduler->UpdateAlarm();
      // cout << " notified " << target_scheduler->name_ << endl;
    }
  }
}

void SchedulerWeaver::AssignCoflowsToSchedulers(
    vector<Coflow *> &parent_coflows, vector<Scheduler *> &schedulers,
    map<Scheduler *, vector<Coflow *> *> *scheduler_to_children_coflows) {

  for (Coflow *parent:parent_coflows) {
    vector<Flow *> sorted_flows = *parent->GetFlows();
    switch (flow_order_mode_) {
      case FLOW_ORDER_BEST: {
        SortAndRangeShuffle(parent, &sorted_flows, [](Flow *l, Flow *r) {
          return l->GetBitsLeft() < r->GetBitsLeft();
        });
        break;
      }
      case FLOW_SIZE_LARGE_FIRST: {
        std::stable_sort(sorted_flows.begin(), sorted_flows.end(),
                         [](Flow *l, Flow *r) {
                           return l->GetBitsLeft() > r->GetBitsLeft();
                         });
        break;
      }
      case FLOW_SIZE_SMALL_FIRST: {
        std::stable_sort(sorted_flows.begin(), sorted_flows.end(),
                         [](Flow *l, Flow *r) {
                           return l->GetBitsLeft() < r->GetBitsLeft();
                         });
        break;
      }
      case FLOW_SRC_DST_IDX_SMALL_FIRST:
        std::stable_sort(sorted_flows.begin(), sorted_flows.end(),
                         [](Flow *l, Flow *r) {
                           return l->GetSrc() == r->GetSrc() ?
                                  l->GetSrc() < r->GetSrc() :
                                  l->GetDest() < r->GetDest();
                         });
        break;
      case FLOW_ORDER_RANDOM: {
        std::mt19937 engine(parent->coflow_rand_seed_);
        std::shuffle(sorted_flows.begin(), sorted_flows.end(), engine);
        break;
      }
      case FLOW_ORDER_NONE: {
        std::cerr << "WARNING: UNKNOWN flow_order_mode_ FLOW_ORDER_NONE\n";
        break;
      }
    }
    // now begin to assign path
    typedef struct Scheduler_Profile {
      Scheduler_Profile() : max_src_sum_bit(0), max_dst_sum_bit(0) {}
      Scheduler_Profile(const map<int, double> &current_load_src_sum_bit,
                        const map<int, double> &current_load_dst_sum_bit) :
          src_sum_bit(current_load_src_sum_bit),
          dst_sum_bit(current_load_dst_sum_bit),
          max_src_sum_bit(MaxMap(current_load_src_sum_bit)),
          max_dst_sum_bit(MaxMap(current_load_dst_sum_bit)) {}
      map<int, double> src_sum_bit, dst_sum_bit;
      double max_src_sum_bit, max_dst_sum_bit;
      vector<Flow *> assigned_flows; // to be added
    } Profile;
    map<Scheduler *, Profile> scheduler_profile;
    for (Flow *flow: sorted_flows) {
      if (!flow->HasDemand()) {
        // For dummy flows with no demand, we have to assign a scheduler so as
        // to clear its demand, if any, in the child scheduler's Transmit().
        Scheduler *scheduler_for_dummy_flow = schedulers[0];
        assert(scheduler_for_dummy_flow);
        Profile &profile = scheduler_profile[scheduler_for_dummy_flow];
        profile.assigned_flows.push_back(flow);
        continue;
      }
      Scheduler *best_scheduler = nullptr;
      double best_cct = -1;
      // if is_critical, this flow's placement may effectively increase cct.
      bool is_critical = false;
      // look for a scheduler (switch) for this flow
      for (Scheduler *this_scheduler:schedulers) {
        Profile &profile = scheduler_profile[this_scheduler];
        double this_sum = max(
            max(profile.max_src_sum_bit,
                flow->GetBitsLeft() + profile.src_sum_bit[flow->GetSrc()]),
            max(profile.max_dst_sum_bit,
                flow->GetBitsLeft() + profile.dst_sum_bit[flow->GetDest()]));
        if (this_sum > max(profile.max_src_sum_bit, profile.max_dst_sum_bit)) {
          // final cct might be increased when any child coflow's cct is increased.
          is_critical = true;
        }
        double this_cct = this_sum / this_scheduler->SCHEDULER_LINK_RATE_BPS_;
        if (this_cct < best_cct || !best_scheduler) {
          best_cct = this_cct;
          best_scheduler = this_scheduler;
        }
      }
      if (!is_critical && flow_order_mode_ != FlowOrderMode::FLOW_ORDER_BEST) {
        if (non_critical_mode_ == NON_CRITICAL_MIN_BN) {
          // best_scheduler remain the same, i.e. the switch with min bottleneck
        } else if (non_critical_mode_ == NON_CRITICAL_RATIO_LB) {
          // when this flow is not critical to effectively increase cct
          // regardless of its placement, we pick the switch with the
          // min max(src_load, dst_load)/capacity on the flow's src and dst.
          best_scheduler = nullptr;
          double best_ratio = -1;
          for (Scheduler *this_scheduler:schedulers) {
            Profile &profile = scheduler_profile[this_scheduler];
            double load = max(profile.src_sum_bit[flow->GetSrc()],
                              profile.dst_sum_bit[flow->GetDest()]);
            double ratio = (flow->GetBitsLeft() + load) // projected load
                / this_scheduler->SCHEDULER_LINK_RATE_BPS_;
            if (best_ratio > ratio || !best_scheduler) {
              best_ratio = ratio;
              best_scheduler = this_scheduler;
            }
            if (debug_level_ >= 2) {
              cout << this_scheduler->name_ << " ratio=" << ratio << endl;
            }
          }
        } else if (non_critical_mode_ == NON_CRITICAL_RANDOM) {
          int run_seed = 12;
          int flow_unique_rand_seed = flow->GetParentCoflow()->GetJobId()
              + flow->GetSrc() + flow->GetDest() + run_seed;
          vector<Scheduler *> rv_values;
          vector<int> rv_prob;
          for (Scheduler *scheduler:schedulers) {
            rv_values.push_back(scheduler);
            // scale link rate to Mbps to fit in prob range of int
            rv_prob.push_back(int(scheduler->SCHEDULER_LINK_RATE_BPS_ / 1e6));
          }
          std::discrete_distribution<int>
              distribution(rv_prob.begin(), rv_prob.end());
          std::mt19937 generator(flow_unique_rand_seed);
          best_scheduler = rv_values[distribution(generator)];
        } else {
          std::cerr << "WARNING: UNKNOWN non_critical_mode_"
                    << non_critical_mode_ << endl;
        } // end of non_critical_mode_ branches.
        if (debug_level_ >= 2) {
          cout << parent->GetName() << " non critical " << flow->toString()
               << " assigned to " << best_scheduler->name_ << endl;
        }
      }
      assert(best_scheduler);
      // for logging purposes only.
      flow->assigned_scheduler_name_ = best_scheduler->name_;
      flow->assigned_scheduler_ = best_scheduler;
      // update profile for the scheduler picked for the flow
      // update load to reflact current assignment
      Profile &profile = scheduler_profile[best_scheduler];
      profile.assigned_flows.push_back(flow);
      profile.src_sum_bit[flow->GetSrc()] += flow->GetBitsLeft();
      profile.dst_sum_bit[flow->GetDest()] += flow->GetBitsLeft();
      profile.max_src_sum_bit = max(profile.max_src_sum_bit,
                                    profile.src_sum_bit[flow->GetSrc()]);
      profile.max_dst_sum_bit = max(profile.max_dst_sum_bit,
                                    profile.dst_sum_bit[flow->GetDest()]);
      // debug
      if (debug_level_ >= 3) {
        cout << "[SchedulerWeaver::AssignCoflowsToSchedulers] "
             << parent->GetName() << " " << flow->toString()
             << (is_critical ? "(critical)" : "(non-critical)")
             << " assigned to " << best_scheduler->name_ << endl;
      }
    } // for flows

    // done with assigning flows within this coflow to different scheduler
    // now generate childeren coflows
    for (auto &scheuler_profile_pair : scheduler_profile) {
      if (!scheuler_profile_pair.second.assigned_flows.empty()) {
        Scheduler *scheduler = scheuler_profile_pair.first;
        Coflow *child = CreateChildCoflowFromParentFlows(
            scheduler, parent, scheuler_profile_pair.second.assigned_flows);
        scheduler_to_children_coflows->operator[](scheduler)->push_back(child);
      }
    }

    // perform logging if two nets
    if (schedulers.size() == 2) {
      for (auto &scheuler_profile_pair : scheduler_profile) {
        if (!scheuler_profile_pair.second.assigned_flows.empty()) {
          Scheduler *scheduler = scheuler_profile_pair.first;
          bool is_on_main = (scheduler == schedulers_[0].get());
          for (Flow *flow: scheuler_profile_pair.second.assigned_flows) {
            long bits_on_main = is_on_main ? flow->GetBitsLeft() : 0L;
            long bits_on_side = flow->GetBitsLeft() - bits_on_main;
            flow->SetBitsThruHybrid(bits_on_main, bits_on_side);
//            cout << flow->GetParentCoflow()->GetName() << " "
//                 << flow->toString() << " assigned to " << scheduler->name_
//                 << " bits_on_main " << bits_on_main << " bits_on_side "
//                 << bits_on_side << endl;
          }
        }
      }

    }// if (schedulers.size() == 2)

  } // for parent in parent_coflows
}

// sort sorted_flows, in place, based on compare, and then shuffle the flows in
// the same range.
template<typename Comparator>
void SchedulerWeaver::SortAndRangeShuffle(Coflow *parent,
                                            vector<Flow *> *sorted_flows,
                                            Comparator compare) {
  // first sort from
  std::stable_sort(sorted_flows->begin(), sorted_flows->end(), compare);
  // then shuffle each range
  std::mt19937 engine(parent->coflow_rand_seed_);
  for (vector<Flow *>::iterator range_head = sorted_flows->begin();
       range_head != sorted_flows->end();) {
    std::pair<std::vector<Flow *>::iterator, std::vector<Flow *>::iterator>
        bounds = std::equal_range(range_head, sorted_flows->end(),
                                  *range_head, compare);
    if (debug_level_ >= 4) {
      cout << "head " << (*bounds.first)->toString();
      if (bounds.second != sorted_flows->end()) {
        cout << " tail " << (*bounds.second)->toString();
      }
      cout << endl;
    }
    std::shuffle(bounds.first, bounds.second, engine);
    range_head = bounds.second;
  }
}

Coflow *SchedulerWeaver::CreateChildCoflowFromParentFlows(
    Scheduler *scheduler, Coflow *parent,
    const vector<Flow *> flows_from_parent) {
  Coflow *child = CreateChildCoflowFromSubFlows(parent, flows_from_parent);
  // warning: child coflow has no valid id. Since coflow id is not used in
  // scheduling, we use coflow name to visually distinguish children
  // coflows (one children coflow for one childre scheduler in the
  // weaver-switch model.
  // child->SetJobId(parent->GetJobId() * 1000 + );
  child->SetName(to_string(parent->GetJobId()) + "." + scheduler->name_);
  // debug
  // cout << "child coflow " << child->GetName()
  // << " assigned to " << scheduler->name_ << endl;
  return child;
}

void SchedulerWeightedRandom::AssignCoflowsToSchedulers(
    vector<Coflow *> &parent_coflows, vector<Scheduler *> &schedulers,
    map<Scheduler *, vector<Coflow *> *> *scheduler_to_children_coflows) {

  vector<Scheduler *> rv_values;
  vector<int> rv_prob;
  for (Scheduler *scheduler:schedulers) {
    rv_values.push_back(scheduler);
    // scale link rate to Mbps to fit in prob range of int
    rv_prob.push_back(int(scheduler->SCHEDULER_LINK_RATE_BPS_ / 1e6));
  }

  for (Coflow *parent:parent_coflows) {
    map<Scheduler *, vector<Flow *>> scheduler_to_flows;
    for (Flow *flow: *parent->GetFlows()) {
      // we pick the switch propitonally to the link capacity.
      // (parent coflow job id, src, dst) is unique for each coflow
      int flow_unique_rand_seed = flow->GetParentCoflow()->GetJobId()
          + flow->GetSrc() + flow->GetDest() + run_seed_;
      std::mt19937 generator(flow_unique_rand_seed);
      std::discrete_distribution<int>
          distribution(rv_prob.begin(), rv_prob.end());
      Scheduler *target_scheduler = rv_values[distribution(generator)];
      scheduler_to_flows[target_scheduler].push_back(flow);
      if (debug_level_ >= 2) {
        cout << flow->toString() << " assigned to " << target_scheduler->name_
             << endl;
      }
    } // for flow
    // done with assigning flows within this coflow to different scheduler
    // now generate childeren coflows
    for (const auto &scheuler_flows_pair : scheduler_to_flows) {
      if (!scheuler_flows_pair.second.empty()) {
        Scheduler *scheduler = scheuler_flows_pair.first;
        Coflow *child = CreateChildCoflowFromParentFlows(
            scheduler, parent, scheuler_flows_pair.second);
        scheduler_to_children_coflows->operator[](scheduler)->push_back(child);
      }
    } // for scheuler_flows_pair : scheduler_to_flows

  } // for each coflow
}