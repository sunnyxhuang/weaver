//
// Created by Xin Sunny Huang on 6/22/17.
//

#include <algorithm> // needed for find in stable_sort()

#include "events.h"
#include "global.h"
#include "scheduler.h"
#include "util.h"
#include "coflow.h"

SchedulerHybrid::SchedulerHybrid(vector<Scheduler*>& schedulers) {
  //initialized all dummy simulators and schedulers.
  for (Scheduler* scheduler: schedulers) {
    schedulers_.push_back(unique_ptr<Scheduler>(scheduler));
    // schedulers_.push_back(std::unique_ptr<Scheduler>(SchedulerFactory::Get(name)));
    // dummy_simulators_.push_back(std::unique_ptr<Simulator>(new Simulator()));
    // schedulers_.back()->InstallSimulator(dummy_simulators_.back().get());
  }
}

void SchedulerHybrid::InstallSimulator(Simulator* simulator) {
  m_simPtr = simulator;
  for (unique_ptr<Scheduler>& scheduler: schedulers_) {
    scheduler->InstallSimulator(m_simPtr);
  }
}

void SchedulerHybrid::SchedulerAlarmPortal(double alarm_time) {

  if (m_currentTime > alarm_time) {
    cerr << "[SchedulerOptc::SchedulerAlarmPortal] ERROR: "
         << "m_currentTime (" << m_currentTime << ") "
         << "> alarmTime (" << alarm_time << ")";
    exit(-1); // while
  }

  for (const unique_ptr<Scheduler>& scheduler: schedulers_) {
    if (scheduler->m_currentTime > alarm_time) {
      cerr << "[SchedulerHybrid::SchedulerAlarmPortal] ERROR:"
           << " scheduler's time " << scheduler->m_currentTime
           << " ahead of alarm time " << alarm_time << endl;
      // exit(-1);
    } else {
      //      cout << "[SchedulerHybrid::SchedulerAlarmPortal] calling TX "
      //           << scheduler->m_currentTime << "->" << alarm_time << endl;

      // instead of perform Transmission in each separate scheduler, we invoke
      // transmission here to synchronize all scheduler's time. We don't bother
      // to peek into each scheduler to see whether salvage transmission is
      // needed. It is possible to do so to avoid scheduling glitches.
      bool has_flow_finished = scheduler->Transmit(
          scheduler->m_currentTime, alarm_time,
          /*basic = */true, /*local = */false, /*salvage = */false);
      // advance scheduler's time since we have perform transmission.
      scheduler->m_currentTime = alarm_time;
    }
  } // for schedulers_
  if (m_currentTime < alarm_time) {
    // at last, we proceed our time.
    m_currentTime = alarm_time;
  }

  while (!m_myTimeLine->isEmpty()) {
    Event* current_event = m_myTimeLine->PeekNext();

    if (alarm_time < current_event->GetEventTime()) break;

    cout << "[SchedulerHybrid::SchedulerAlarmPortal] " << alarm_time
         << " working on " << current_event->GetEventType() << endl;

    // wake up the corresponding scheduler (from my_timeline)
    switch (current_event->GetEventType()) {
      case COFLOW_ARRIVE: {
        CoflowArrive();
        break;
      }
      default:cerr << "Event not processed!\n";
        break;
    }

    Event* e2d = m_myTimeLine->PopNext();
    if (e2d != current_event) {
      cout << "[Simulator::Run] error: e2d != currentEvent " << endl;
      exit(-1);
    }
    delete e2d;
  }

  // update alarm from my_timeline to (external and real) simulator.
  // UpdateAlarm();
}

void SchedulerHybrid::CoflowFinishCallBack(double finish_time) {
  for (const unique_ptr<Scheduler>& scheduler: schedulers_) {
    scheduler->CoflowFinishCallBack(finish_time);
  }
}

void SchedulerHybrid::FlowFinishCallBack(double finish_time) {
  for (const unique_ptr<Scheduler>& scheduler: schedulers_) {
    scheduler->FlowFinishCallBack(finish_time);
  }
}

void SchedulerHybrid::CoflowArrive() {
  EventCoflowArrive* event = (EventCoflowArrive*) m_myTimeLine->PeekNext();
  if (event->GetEventType() != COFLOW_ARRIVE) {
    cout << "[SchedulerHybrid::CoflowArrive] error: "
         << " the event type is not COFLOW_ARRIVE!" << endl;
    return;
  }

  vector<Coflow*>* coflows_to_main = new vector<Coflow*>();
  vector<Coflow*>* coflows_to_backup = new vector<Coflow*>();
  vector<Coflow*> coflows_to_assign = *event->m_cfpVp;

  long main_link_rate_bps = schedulers_.front()->SCHEDULER_LINK_RATE_BPS_;
  long backup_link_rate_bps = schedulers_.back()->SCHEDULER_LINK_RATE_BPS_;

  if (main_link_rate_bps <= 0) {
    coflows_to_backup->operator=(coflows_to_assign);
  } else if (backup_link_rate_bps <= 0) {
    coflows_to_main->operator=(coflows_to_assign);
  } else {
    AssignCoflowsToSchedulers(coflows_to_assign,
                              main_link_rate_bps, backup_link_rate_bps,
                              coflows_to_main, coflows_to_backup);
  }
  if (!coflows_to_main->empty()) {
    // cout << "sunny adding fast coflows\n";
    Scheduler* target_scheduler = schedulers_.front().get();
    target_scheduler->m_myTimeLine->AddEvent
        (new EventCoflowArrive(m_currentTime, coflows_to_main));
    target_scheduler->UpdateAlarm();
  }
  if (!coflows_to_backup->empty()) {
    // cout << "sunny adding slow coflows\n";
    Scheduler* target_scheduler = schedulers_.back().get();
    target_scheduler->m_myTimeLine->AddEvent(
        new EventCoflowArrive(m_currentTime, coflows_to_backup));
    target_scheduler->UpdateAlarm();
  }
}

void SchedulerHybrid::AssignCoflowsToSchedulers(
    const vector<Coflow*>& coflows_to_assign,
    long main_link_rate_bps, long backup_link_rate_bps,
    vector<Coflow*>* coflows_to_main, vector<Coflow*>* coflows_to_backup) {

  for (Coflow* coflow: coflows_to_assign) {
    if (ShouldGoBackup(coflow, main_link_rate_bps, backup_link_rate_bps)) {
      // cout << " BackupNet Coflow # " << coflow->GetJobId() << endl;
      coflows_to_backup->push_back(coflow);
    } else {
      // cout << " MainNet Coflow # " << coflow->GetJobId() << endl;
      coflows_to_main->push_back(coflow);
    }
  }
}

void SchedulerSplit::AssignCoflowsToSchedulers(
    const vector<Coflow*>& coflows_to_assign,
    long main_link_rate_bps, long backup_link_rate_bps,
    vector<Coflow*>* coflows_to_main, vector<Coflow*>* coflows_to_backup) {
  for (Coflow* coflow: coflows_to_assign) {
    vector<Flow*> flows_to_main, flows_to_backup;
    SplitCoflow(coflow, main_link_rate_bps, backup_link_rate_bps,
                &flows_to_main, &flows_to_backup);
    // debug message
    cout << "Root Coflow # " << coflow->GetJobId() << ", "
         << "to main with " << flows_to_main.size() << " flows, "
         << "to backup with " << flows_to_backup.size() << " flows\n";
    if (flows_to_main.size() > 0) {
      Coflow* child = CreateChildCoflowFromSubFlows(coflow, flows_to_main);
      child->SetJobId(coflow->GetJobId() * 1000 + 1);
      coflows_to_main->push_back(child);
    }
    if (flows_to_backup.size() > 0) {
      Coflow* child = CreateChildCoflowFromSubFlows(coflow, flows_to_backup);
      child->SetJobId(coflow->GetJobId() * 1000 + 2);
      coflows_to_backup->push_back(child);
    }
  }
}

void SchedulerSplit::SplitCoflow(Coflow* coflow_to_split,
                                 long main_link_rate_bps,
                                 long backup_link_rate_bps,
                                 vector<Flow*>* flows_to_main,
                                 vector<Flow*>* flows_to_backup) {
  flows_to_main->clear();
  flows_to_backup->clear();
  vector<Flow*> sorted_flows = *coflow_to_split->GetFlows();
  // TODO: try many other ways to sort and split flows.
  std::stable_sort(sorted_flows.begin(), sorted_flows.end(), flowCompBitsLeft);
  double load_on_main = 0, load_on_backup = 0;
  for (Flow* flow: sorted_flows) {
    double main_score = load_on_main / (main_link_rate_bps / (double) 1e9);
    double backup_score = load_on_backup
        / (backup_link_rate_bps / (double) 1e9);
    // TODO: try network load aware splitting.
    if (main_score < backup_score || (main_score == backup_score
        && main_link_rate_bps > backup_link_rate_bps)) {
      flows_to_main->push_back(flow);
      load_on_main += flow->GetBitsLeft();
    } else {
      flows_to_backup->push_back(flow);
      load_on_backup += flow->GetBitsLeft();
    }
  } // for flow : sorted_flows
}

ChildCoflow* SchedulerSplit::CreateChildCoflowFromSubFlows(
    Coflow* parent_coflow, const vector<Flow*>& flows_in_coflow) {
  if (flows_in_coflow.size() <= 0) return nullptr;
  ChildCoflow* result = new ChildCoflow(parent_coflow);
  for (Flow* flow: flows_in_coflow) {
    result->AddFlow(flow);
  }
  return result;
}

bool SchedulerHybrid::ShouldGoBackup(Coflow* coflow_to_go,
                                     long main_link_rate_bps,
                                     long backup_link_rate_bps) {
  //  if (main_link_rate_bps <= 0) return true;
  //  if (backup_link_rate_bps <= 0) return false;

  // Traffic goes to EPS by default
  cout << "[SchedulerHybrid::ShouldGoBackup] "
       << "by default all coflows go through main network.\n";
  // always go through backup (usually OCS) if main scheduler (usually EPS) has
  // no resource; otherwise, main scheduler(usually EPS).
  return false;
}
