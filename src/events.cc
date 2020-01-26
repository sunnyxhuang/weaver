
#include <iomanip>
#include <map>

#include "events.h"

using namespace std;

ostream& operator<<(std::ostream& out, const EventType value) {
  static map<EventType, string> strings;
  if (strings.size() == 0) {
#define INSERT_ELEMENT(p) strings[p] = #p
    INSERT_ELEMENT(NO_EVENT);
    INSERT_ELEMENT(SUB_JOB);
    INSERT_ELEMENT(WORKER_FINISH);
    INSERT_ELEMENT(MSG_ADD_FLOWS);
    INSERT_ELEMENT(MSG_ADD_COFLOWS);
    INSERT_ELEMENT(ALARM_TRAFFIC);
    INSERT_ELEMENT(FLOW_FINISH);
    INSERT_ELEMENT(COFLOW_ARRIVE);
    INSERT_ELEMENT(FLOW_ARRIVE);
    INSERT_ELEMENT(RESCHEDULE);
    INSERT_ELEMENT(ORDER_CIRCUIT);
    INSERT_ELEMENT(APPLY_CIRCUIT);
    INSERT_ELEMENT(APPLY_NEW_SCHEDULE);
    INSERT_ELEMENT(SCHEDULE_END);
    INSERT_ELEMENT(MSG_TRAFFIC_FINISH);
    INSERT_ELEMENT(ALARM_SCHEDULER);
#undef INSERT_ELEMENT
  }
  return out << strings[value];
}

bool
TimeLine::AddEvent(Event* ePtr) {
  if (!ePtr || ePtr->GetEventTime() < 0) {
    return false;
  }
  double tm = ePtr->GetEventTime();
  vector<Event*>::iterator it;
  for (it = m_timeline.begin(); it != m_timeline.end(); it++) {
    if (tm < (*it)->GetEventTime()) {
      m_timeline.insert(it, ePtr);
      break;
    }
  }
  if (it == m_timeline.end()) {
    m_timeline.push_back(ePtr);
  }
  // TODO: remove debug message
  //  cout << string(FLOAT_TIME_WIDTH + 2, ' ')
  //       << "[TimeLine::AddEvent] add event " << ePtr->GetEventType()
  //       << " at " << ePtr->GetEventTime() << endl;
  return true;
}

Event*
TimeLine::PeekNext() {
  if (!m_timeline.empty()) {
    return m_timeline.front();
  } else {
    cout << "[TimeLine::PeekNext] Error: timeline empty" << endl;
    return NULL;
  }
}

Event*
TimeLine::PopNext() {
  if (!m_timeline.empty()) {
    Event* ePtr2pop = m_timeline.front();
    m_timeline.erase(m_timeline.begin());
    return ePtr2pop;
  } else {
    cout << "[TimeLine::PopNext] Error: timeline empty" << endl;
    return NULL;
  }
}

bool
TimeLine::isEmpty() {
  return m_timeline.empty();
}

bool
SchedulerTimeLine::RemoveMultipleEvent(EventType tp) {
  if (m_timeline.size() <= 0) {
    return true;
  }
  if (tp != APPLY_CIRCUIT) {
    // sanity check the legal singular event type
    cout << string(FLOAT_TIME_WIDTH + 2, ' ')
         << "[Simulator::RemoveSingularEvent] ERROR: try to remove " << tp
         << "but it can NOT be multiple!" << endl;
    return false;
  }
  // skip the event we are handling this type of event
  for (vector<Event*>::iterator
           it = m_timeline.begin() + 1; /*remove any NON-CURRENT*/
       it != m_timeline.end();) {
    if ((*it)->GetEventType() == tp) {
      delete *it;
      it = m_timeline.erase(it);
    } else {
      it++;
    }
  }
  return true;
}

bool
SchedulerTimeLine::RemoveSingularEvent(EventType tp) {
  if (m_timeline.size() <= 0) {
    return true;
  }
  if (tp != FLOW_FINISH
      && tp != APPLY_CIRCUIT
      && tp != ORDER_CIRCUIT
      && tp != APPLY_NEW_SCHEDULE
      && tp != RESCHEDULE
      && tp != SCHEDULE_END) {
    // sanity check the legal singular event type
    cout << string(FLOAT_TIME_WIDTH + 2, ' ')
         << "[Simulator::RemoveSingularEvent] ERROR: try to remove " << tp
         << "but it is NOT singular!" << endl;
    return false;
  }
  vector<Event*>::iterator it;
  if ((*m_timeline.begin())->GetEventType() == tp) {
    // we are now handling this type of event
    // there should not be other event with type *tp*
    // in the event vector
    return true;
  }
  for (it = m_timeline.begin() + 1; /*remove any NON-CURRENT*/
       it != m_timeline.end();) {
    if ((*it)->GetEventType() == tp) {
      // important: delete event pointer
      delete *it;
      it = m_timeline.erase(it);
      // there should be no more than one event with type
      // tp in the event vector
      break;
    } else {
      it++;
    }
  }
  return true;
}

void
TimeLine::Print(void) {
  int eventCount = 0;
  for (vector<Event*>::iterator it = m_timeline.begin();
       it != m_timeline.end(); it++) {
    cout << string(FLOAT_TIME_WIDTH + 2, ' ')
         << "[TimeLine::Print] "
         << "TimeLine(" << eventCount++ << ") " << " to do "
         << (*it)->GetEventType() << " at " << (*it)->GetEventTime() << "s"
         << endl;
  }
}

Simulator::Simulator() {

  m_currentTime = 0;

  m_schedulerPtr = NULL;
  m_trafficPtr = NULL;
}

bool
Simulator::InstallScheduler(const std::string& schedulerName) {
  // can only be installed once
  if (m_schedulerPtr) return false;
  m_schedulerPtr = SchedulerFactory::Get(schedulerName);
  if (m_schedulerPtr) {
    // This is a valid scheduler
    m_schedulerPtr->InstallSimulator(this);
    return true;
  }

  return false;
}

bool
Simulator::InstallTrafficGen(std::string trafficProducerName,
                             DbLogger* db_logger) {
  // can only be installed once
  if (m_trafficPtr) return false;
  m_trafficPtr = TrafficFactory::Get(trafficProducerName, db_logger);
  if (m_trafficPtr) {
    m_trafficPtr->InstallSimulator(this);
    return true;
  }
  return false;
}

Simulator::~Simulator() {
  m_timeline.clear();
  delete m_schedulerPtr;
  delete m_trafficPtr;
}

void
Simulator::UpdateSchedulerAlarm(EventNotifyScheduler* new_event) {

  if (m_timeline.empty()) {
    m_timeline.push_back(new_event);
    return;
  }
  // from the event next to the current
  for (vector<Event*>::iterator tlIt = m_timeline.begin() + 1;
       tlIt != m_timeline.end(); tlIt++) {
    if ((*tlIt)->GetEventType() == ALARM_SCHEDULER) {
      EventNotifyScheduler* old_event = (EventNotifyScheduler*) *tlIt;
      if (old_event->scheduler == new_event->scheduler) {
        // There should be no more than one alarm for each scheduler.
        delete *tlIt;
        m_timeline.erase(tlIt);
        break;
      }
    }
  }

  // insert the new event
  AddEvent(new_event);
}

void
Simulator::UpdateTrafficAlarm(Event* ep) {
  if (m_timeline.empty()) {
    m_timeline.push_back(ep);
    return;
  }
  // from the event next to the current
  for (vector<Event*>::iterator tlIt = m_timeline.begin() + 1;
       tlIt != m_timeline.end(); tlIt++) {
    if ((*tlIt)->GetEventType() == ALARM_TRAFFIC) {
      delete *tlIt;
      m_timeline.erase(tlIt);
      break;
      // there should be no more than 1 scheduler alarm
    }
  }

  // insert the new event
  AddEvent(ep);
}

void
Simulator::Run() {
  if (!m_schedulerPtr) {
    cout << "[Simulator::Run] Error: "
         << "Please install scheduler first!" << endl;
    return;
  }
  if (!m_trafficPtr) {
    cout << "[Simulator::Run] Error: "
         << "Please install traffic generator first!" << endl;
    return;
  }

  m_trafficPtr->NotifySimStart();

  while (!m_timeline.empty()) {
    // Step 1: peek for time of next event
    Event* nextEvent = PeekNext();
    double nextEventTime = nextEvent->GetEventTime();

    if (nextEventTime < m_currentTime) {
      cout << "Error: event time is earlier" <<
           " than current time!" << endl;
    }

    // Step 2: adjust clock
    m_currentTime = nextEventTime;

    // Step 4: execute next event
    switch (nextEvent->GetEventType()) {
      case ALARM_TRAFFIC:m_trafficPtr->TrafficGenAlarmPortal(m_currentTime);
        break;
      case MSG_TRAFFIC_FINISH:DoNotifyTrafficFinish();
        break;
      case ALARM_SCHEDULER: {
        static_cast<EventNotifyScheduler*>(
            nextEvent)->scheduler->SchedulerAlarmPortal(m_currentTime);
        // m_schedulerPtr->SchedulerAlarmPortal(m_currentTime);
      }
        break;
      case MSG_ADD_FLOWS:DoNotifyAddFlows();
        break;
      case MSG_ADD_COFLOWS:DoNotifyAddCoflows();
        break;
      default:break;
    }

    // Step 5: rm event executed
    Event* e2p = PopNext();
    if (e2p != nextEvent) {
      cout << "[Simulator::Run] error: "
           << " e2p != currentEvent " << endl;
    }
    delete nextEvent;
  }
  // end of simulation.
  m_trafficPtr->NotifySimEnd();
  m_schedulerPtr->NotifySimEnd();
}

void
Simulator::DoNotifyTrafficFinish() {
  //m_trafficPtr->NotifyTrafficReq();
  MsgEventTrafficFinish* event
      = static_cast<MsgEventTrafficFinish*>(PeekNext());
  if (event->GetEventType() != MSG_TRAFFIC_FINISH) {
    cout << "[Simulator::DoNotifyTrafficFinish] error: "
         << " the event type is not MSG_TRAFFIC_FINISH!" << endl;
    return;
  }

  m_trafficPtr->NotifyTrafficFinish(m_currentTime,
                                    event->coflows,
                                    event->flows);
}

void
Simulator::DoNotifyAddFlows() {
  MsgEventAddFlows* addFlowsEvent = (MsgEventAddFlows*) PeekNext();
  if (addFlowsEvent->GetEventType() != MSG_ADD_FLOWS) {
    cout << "[Simulator::DoNotifyAddFlows] error: "
         << " the event type is not MSG_ADD_FLOWS!" << endl;
    return;
  }
  m_schedulerPtr->NotifyAddFlows(m_currentTime);
}

void
Simulator::DoNotifyAddCoflows() {
  //m_schedulerPtr->NotifyScheduleEnd();
  MsgEventAddCoflows* addCoflowsEvent = (MsgEventAddCoflows*) PeekNext();
  if (addCoflowsEvent->GetEventType() != MSG_ADD_COFLOWS) {
    cout << "[Simulator::DoNotifyAddCoflows] error: "
         << " the event type is not MSG_ADD_COFLOWS!" << endl;
    return;
  }
  m_schedulerPtr->NotifyAddCoflows(m_currentTime, addCoflowsEvent->m_cfpVp);

}