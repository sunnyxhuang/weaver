//
//  scheduler.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/21/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#include <iomanip>
#include <cfloat> 
#include <sys/time.h>
#include <string.h>

#include "coflow.h"
#include "events.h"
#include "global.h"
#include "scheduler.h"
#include "util.h"

#define MWM_RANGE 100000000 //2147483647 = 2,147,483,647

using namespace std;

///////////////////////////////////////////////////////
////////////// Code for base class Scheduler
///////////////////////////////////////////////////////
int Scheduler::instance_count_ = 0;
const double Scheduler::INVALID_RATE_ = DBL_MAX;

Scheduler::Scheduler(long scheduler_link_rate) :
    SCHEDULER_LINK_RATE_BPS_(scheduler_link_rate) {

  instance_count_++;

  m_simPtr = NULL;

  m_currentTime = 0;
  m_myTimeLine = new SchedulerTimeLine();
  m_coflowPtrVector = vector<Coflow *>();

  m_nextElecRate = map<long, long>();
  m_nextOptcRate = map<long, long>();
}

Scheduler::~Scheduler() {
  delete m_myTimeLine;
}

void
Scheduler::UpdateAlarm() {
  // update alarm for scheduler on simulator
  if (!m_myTimeLine->isEmpty()) {
    Event *nextEvent = m_myTimeLine->PeekNext();
    double nextTime = nextEvent->GetEventTime();
    // Event *schedulerAlarm = new Event(ALARM_SCHEDULER, nextTime);
    m_simPtr->UpdateSchedulerAlarm(new EventNotifyScheduler(nextTime, this));
  }
}

void
Scheduler::NotifySimEnd() {
  return;
  cout << "[Scheduler::NotifySimEnd()] is called." << endl;
  for (vector<Coflow *>::iterator cfIt = m_coflowPtrVector.begin();
       cfIt != m_coflowPtrVector.end(); cfIt++) {

    vector<Flow *> *flowVecPtr = (*cfIt)->GetFlows();

    for (vector<Flow *>::iterator fpIt = flowVecPtr->begin();
         fpIt != flowVecPtr->end(); fpIt++) {

      if ((*fpIt)->GetBitsLeft() <= 0) {
        // such flow has finished
        continue;
      }

      cout << "[Scheduler::NotifySimEnd()] flow [" << (*fpIt)->GetFlowId()
           << "] "
           << "(" << (*fpIt)->GetSrc() << "=>" << (*fpIt)->GetDest() << ") "
           << (*fpIt)->GetSizeInBit() << " bytes "
           << (*fpIt)->GetBitsLeft() << " bytes left "
           << (*fpIt)->GetElecRate() << " bps" << endl;

    }
  }
}

bool Scheduler::Transmit(double startTime, double endTime,
                         bool basic, bool local, bool salvage) {
  // cout << "[Scheduler::Transmit] TX " << startTime << "->" << endTime << endl;

  bool hasCoflowFinish = false;
  bool hasFlowFinish = false;
  bool hasFakeCoflowFinish = false;

  vector<Coflow *> finished_coflows;
  vector<Flow *> finished_flows;

  map<int, long> validate_tx_src_bits, validate_tx_dst_bits;
  map<int, int> validate_src_flow_num, validate_dst_flow_num;

  for (vector<Coflow *>::iterator cfIt = m_coflowPtrVector.begin();
       cfIt != m_coflowPtrVector.end();) {

    for (Flow *flow : *((*cfIt)->GetFlows())) {

      if (flow->GetBitsLeft() <= 0) {
        // such flow has finished
        continue;
      }

      // tx rate verification debug
      long validate_tx_this_flow_bits = flow->GetBitsLeft();

      // ********* begin tx ****************
      if (basic) {
        flow->Transmit(startTime, endTime);
      }
      // tx rate verification debug // only consider non-local, major tx
      validate_tx_this_flow_bits -= flow->GetBitsLeft();

      if (local) {
        flow->TxLocal();
      }
      if (salvage) {
        flow->TxSalvage();
      }
      // ********* end tx ********************
      validate_tx_src_bits[flow->GetSrc()] += validate_tx_this_flow_bits;
      validate_tx_dst_bits[flow->GetDest()] += validate_tx_this_flow_bits;
      if (validate_tx_this_flow_bits > 0) {
        validate_src_flow_num[flow->GetSrc()]++;
        validate_dst_flow_num[flow->GetDest()]++;
      }

      if (flow->GetBitsLeft() == 0) {
        hasFlowFinish = true;
        (*cfIt)->NumFlowFinishInc();
        flow->SetEndTime(endTime);
        finished_flows.push_back(flow);

        // debug for coflow progress
        if (DEBUG_LEVEL >= 4) {
          cout << fixed << setw(FLOAT_TIME_WIDTH) << endTime << "s Finished "
               << (*cfIt)->GetName() << " " << flow->toString()
               << (*cfIt)->NumFlowsLeft() << " flows left in coflow\n";
        }
      }

      // update coflow account on bytes sent.
      (*cfIt)->AddTxBit(validate_tx_this_flow_bits);
    }

    if ((*cfIt)->IsComplete()) {
      Coflow *finished_coflow = (*cfIt)->GetRootCoflow();
      finished_coflows.push_back(finished_coflow);
      finished_coflow->SetEndTime(endTime);
      hasCoflowFinish = true;
      //cout << string(FLOAT_TIME_WIDTH+2, ' ')
      cout << fixed << setw(FLOAT_TIME_WIDTH) << endTime << "s "
           << "[Scheduler::Transmit] coflow finish! "
           << "Name " << (*cfIt)->GetName() << ", "
           << "root Coflow #" << finished_coflow->GetJobId() << endl;
    }
    if ((*cfIt)->IsComplete()
        || ((*cfIt)->IsFake() && (*cfIt)->IsFakeCompleted())) {
      if ((*cfIt)->IsFake() && (*cfIt)->IsFakeCompleted()) {
        // This fake coflow has done. We can delete it here and the parent
        // coflow will be handled back as finishd coflow.
        hasFakeCoflowFinish = true;
        // cout << "Deleting Fake Coflow #" << (*cfIt)->GetJobId() << endl;
        delete (*cfIt);
      }
      // advance coflow iterator.
      cfIt = m_coflowPtrVector.erase(cfIt);
    } else {
      // jump to next coflow
      cfIt++;
    }
  } // for coflow iterator

  ScheduleToNotifyTrafficFinish(endTime, finished_coflows, finished_flows);

  if (hasCoflowFinish || hasFakeCoflowFinish) {
    CoflowFinishCallBack(endTime);
  } else if (hasFlowFinish) {
    FlowFinishCallBack(endTime);
  }

  if (hasCoflowFinish || hasFlowFinish || hasFakeCoflowFinish) {
    Scheduler::UpdateFlowFinishEvent(endTime);
  }

  // Validate bits transmitted meet constraints.
  // in packet switched network, there might bee 100s of concurrent flows
  // to/from the same port, and each flow tx possibly comes with 1 bit error.
  // Therefore we allow a larger slack for tx constraint check, based on the
  // number of  concurrent flows at a port . In optical net, the number of
  // concurrent flows are less. So a smaller slack is sufficient.
  long bound =
      10 + SCHEDULER_LINK_RATE_BPS_ * NUM_LINK_PER_RACK * (endTime - startTime);
  if (!ValidateLastTxMeetConstraints(
      bound, validate_tx_src_bits, validate_tx_dst_bits,
      validate_src_flow_num, validate_dst_flow_num)) {
    cerr << startTime << "  Warming: Fail to meet tx bound!!!" <<
         endl;
    exit(-1);
  }

  if (hasCoflowFinish || hasFlowFinish || hasFakeCoflowFinish) {
    return true;
  }
  return false;
}

void
Scheduler::ScheduleToNotifyTrafficFinish(double end_time,
                                         vector<Coflow *> &coflows_done,
                                         vector<Flow *> &flows_done) {
  if (coflows_done.empty() && flows_done.empty()) {
    return;
  }

  //  if (coflows_done.empty()
  //      && SPEEDUP_1BY1_TRAFFIC_IN_SCHEDULER
  //      && m_coflowPtrVector.size() == 1) {
  //    // no FCT for hacking mode.
  //    return;
  //  }
  //notify traffic generator of coflow / flow finish
  vector<Coflow *> *finishedCf = new vector<Coflow *>(coflows_done);
  vector<Flow *> *finishedF = new vector<Flow *>(flows_done);
  MsgEventTrafficFinish *msgEventPtr =
      new MsgEventTrafficFinish(end_time, finishedCf, finishedF);
  m_simPtr->AddEvent(msgEventPtr);
}

//returns negative if all flows has finished
//return DBL_MAX if all flows are waiting indefinitely
double
Scheduler::CalcTime2FirstFlowEnd() {
  double time2FirstFinish = DBL_MAX;
  bool hasUnfinishedFlow = false;
  bool finishTimeValid = false;
  for (Coflow *coflow :m_coflowPtrVector) {
    for (Flow *flow : *coflow->GetFlows()) {
      if (flow->GetBitsLeft() <= 0) {
        //such flow has completed
        continue;
      }

      hasUnfinishedFlow = true;
      // calc the min finishing time
      double flowCompleteTime = DBL_MAX;
      if (flow->isThruOptic() && flow->GetOptcRate() > 0) {
        flowCompleteTime = SecureFinishTime(flow->GetBitsLeft(),
                                            flow->GetOptcRate());
      } else if (!flow->isThruOptic() && flow->GetElecRate() > 0) {
        flowCompleteTime = SecureFinishTime(flow->GetBitsLeft(),
                                            flow->GetElecRate());
      }
      if (time2FirstFinish > flowCompleteTime) {
        finishTimeValid = true;
        time2FirstFinish = flowCompleteTime;
      }
    }
  }
  if (hasUnfinishedFlow) {
    if (finishTimeValid) {
      return time2FirstFinish;
    } else {
      //all flows are waiting indefinitely
      return DBL_MAX;
    }
  } else {
    // all flows are finished
    return -DBL_MAX;
  }
}

double Scheduler::UpdateFlowFinishEvent(double baseTime) {

  double time2FirstFinish = CalcTime2FirstFlowEnd();

  if (time2FirstFinish == DBL_MAX) {
    // all flows are waiting indefinitely
    m_myTimeLine->RemoveSingularEvent(FLOW_FINISH);

  } else if (time2FirstFinish == -DBL_MAX) {
    // all flows are done

  } else {
    // valid finishing time
    m_myTimeLine->RemoveSingularEvent(FLOW_FINISH);
    double firstFinishTime = baseTime + time2FirstFinish;
    Event *flowFinishEventPtr = new Event(FLOW_FINISH, firstFinishTime);
    m_myTimeLine->AddEvent(flowFinishEventPtr);
  }

  return time2FirstFinish;
}

void
Scheduler::UpdateRescheduleEvent(double reScheduleTime) {
  m_myTimeLine->RemoveSingularEvent(RESCHEDULE);
  Event *rescheduleEventPtr = new Event(RESCHEDULE, reScheduleTime);
  m_myTimeLine->AddEvent(rescheduleEventPtr);
}

void
Scheduler::NotifyAddFlows(double alarmTime) {
  //FlowArrive(alarmTime);
  EventFlowArrive *msgEp = new EventFlowArrive(alarmTime);
  m_myTimeLine->AddEvent(msgEp);
  UpdateAlarm();
}

void
Scheduler::NotifyAddCoflows(double alarmTime, vector<Coflow *> *cfVecPtr) {
  //CoflowArrive(alarmTime,cfVecPtr);
  EventCoflowArrive *msgEp = new EventCoflowArrive(alarmTime, cfVecPtr);
  m_myTimeLine->AddEvent(msgEp);
  UpdateAlarm();
}

double
Scheduler::SecureFinishTime(long bits, long rate) {
  if (rate == 0) {
    return DBL_MAX;
  }
  double timeLen = (double) bits / (double) rate;

  // more complicated impl below *********
  long bitsleft = 1;
  int delta = 0;
  while (bitsleft > 0) {
    timeLen = (double) (delta + bits) / (double) rate;
    bitsleft = bits - rate * timeLen;
    delta++;
  }
  // more complicated impl above *****

  //if (timeLen < 0.0000000001) return 0.0000000001;
  return timeLen;
}

void
Scheduler::Print(void) {
  return;
  for (vector<Coflow *>::iterator cfIt = m_coflowPtrVector.begin();
       cfIt != m_coflowPtrVector.end(); cfIt++) {
    if (cfIt == m_coflowPtrVector.begin()) {
      cout << fixed << setw(FLOAT_TIME_WIDTH)
           << m_currentTime << "s ";
    } else {
      cout << string(FLOAT_TIME_WIDTH + 2, ' ');
    }

    cout << "[Scheduler::Print] "
         << "Coflow ID " << (*cfIt)->GetCoflowId() << endl;
    (*cfIt)->Print();
  }
}


// copy flow rate from m_nextElecRate & m_nextOptcRate
// and reflect the rate on flow record.
void
Scheduler::SetFlowRate() {
  for (vector<Coflow *>::iterator cfIt = m_coflowPtrVector.begin();
       cfIt != m_coflowPtrVector.end(); cfIt++) {
    vector<Flow *> *flowVecPtr = (*cfIt)->GetFlows();
    for (vector<Flow *>::iterator fpIt = flowVecPtr->begin();
         fpIt != flowVecPtr->end(); fpIt++) {
      //set flow rate
      long flowId = (*fpIt)->GetFlowId();
      long elecBps = MapWithDef(m_nextElecRate, flowId, (long) 0);
      long optcBps = MapWithDef(m_nextOptcRate, flowId, (long) 0);
      (*fpIt)->SetRate(elecBps, optcBps);
    }
  }
}

bool Scheduler::ValidateLastTxMeetConstraints(
    long port_bound_bits,
    const map<int, long> &src_tx_bits, const map<int, long> &dst_tx_bits,
    const map<int, int> &src_flow_num, const map<int, int> &dst_flow_num) {
  for (const auto &src_kv_pair : src_tx_bits) {
    long buget = port_bound_bits
        + FindWithDef(src_flow_num, src_kv_pair.first, 0);
    if (src_kv_pair.second > buget) {
      cerr << "Error in validating TX constraints!!! \n"
           << "src " << src_kv_pair.first << " tx " << src_kv_pair.second
           << " and flows over bound = " << buget << endl;
      return false;
    } else if (DEBUG_LEVEL >= 5) {
      cout << "Valid: src " << src_kv_pair.first
           << " tx " << src_kv_pair.second
           << " < budget = " << buget << endl;
    }
  }

  for (const auto &dst_kv_pair : dst_tx_bits) {
    long buget = port_bound_bits
        + FindWithDef(dst_flow_num, dst_kv_pair.first, 0);
    if (dst_kv_pair.second > buget) {
      cerr << "Error in validating TX constraints!!! \n"
           << "dst " << dst_kv_pair.first << " tx " << dst_kv_pair.second
           << " and flows over bound = " << buget << endl;
      return false;
    } else if (DEBUG_LEVEL >= 5) {
      cout << "Valid: dst " << dst_kv_pair.first
           << " tx " << dst_kv_pair.second
           << " < budget = " << buget << endl;
    }
  }
  // cout << "TX budget is valid." << endl;
  return true;
}
