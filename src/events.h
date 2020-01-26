//
//  events.h
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/20/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//


#ifndef EVENTS_H
#define EVENTS_H

#include <vector>
#include <iostream>

#include "global.h"
#include "scheduler.h"
#include "traffic_generator.h"

using namespace std;

class Flow;
class Coflow;
class Scheduler;
class TrafficGen;
class DbLogger;

std::ostream &operator<<(std::ostream &out, const EventType value);

// base class
class Event {
 public:
  Event(EventType type, double time)
      : m_eventType(type),
        m_eventTime(time) {}
  virtual ~Event() {}
  double GetEventTime() { return m_eventTime; }
  EventType GetEventType() { return m_eventType; }
  virtual void CallBack() { return; }
 private:
  EventType m_eventType;    /*event type*/
  double m_eventTime;    /*event start time*/
};

// for simulator
class MsgEventAddFlows : public Event {
 public:
  MsgEventAddFlows(/*EventType type,*/
      double time) : Event(/*type*/MSG_ADD_FLOWS, time) {};
  ~MsgEventAddFlows() {}
};

class MsgEventAddCoflows : public Event {
 public:
  MsgEventAddCoflows(/*EventType type,*/
      double time,
      vector<Coflow *> *cfpVp) :
      Event(/*type*/ MSG_ADD_COFLOWS, time),
      m_cfpVp(cfpVp) {};
  ~MsgEventAddCoflows() {/*if(coflows) delete coflows;*/}
  /* message body */
  vector<Coflow *> *m_cfpVp; // pointer to the vector of coflow pointer
};

// for scheduler
class EventCoflowArrive : public Event {
 public:
  EventCoflowArrive(/*EventType type,*/
      double time,
      vector<Coflow *> *cfpVp) :
      Event(/*type*/ COFLOW_ARRIVE, time),
      m_cfpVp(cfpVp) {};
  ~EventCoflowArrive() { if (m_cfpVp) delete m_cfpVp; }
  /* message body */
  vector<Coflow *> *m_cfpVp; // pointer to the vector of coflow pointer
};

class EventFlowArrive : public Event {
 public:
  EventFlowArrive(double time) : Event(/*type*/FLOW_ARRIVE, time) {};
  ~EventFlowArrive() {}
};

class MsgEventTrafficFinish : public Event {
 public:
  MsgEventTrafficFinish(/*EventType type,*/
      double time,
      vector<Coflow *> *c,
      vector<Flow *> *f) : Event(/*type*/ MSG_TRAFFIC_FINISH, time),
                           coflows(c), flows(f) {};
  ~MsgEventTrafficFinish() {
    if (coflows)delete coflows;
    if (flows) delete flows;
  }
  /* message body */
  vector<Coflow *> *coflows; // pointer to the vector of coflow pointer
  vector<Flow *> *flows;      // pointer to the vector of flow pointer
};

// Used by SchedulerHybrid to identify the scheduler to invoke.
class EventNotifyScheduler : public Event {
 public:
  EventNotifyScheduler(double time, Scheduler *scheduler) :
      Event(ALARM_SCHEDULER, time), scheduler(scheduler) {};
  ~EventNotifyScheduler() {}
  Scheduler *scheduler; // not owned.
};

class Job;
class JobDesc;

class EventSubmitJobDesc : public Event {
 public:
  EventSubmitJobDesc(
      double time,
      JobDesc *jobPtr) : Event(/*type*/ SUB_JOB, time),
                         m_jobPtr(jobPtr) {}
  ~EventSubmitJobDesc() {}

  JobDesc *m_jobPtr;
};

// base class
class TimeLine {
 public:
  TimeLine() {}
  ~TimeLine() {}
  bool AddEvent(Event *ePtr);
  Event *PeekNext();
  Event *PopNext();
  bool isEmpty();
 protected:
  vector<Event *> m_timeline;
  void Print();
};

// for scheduler
class SchedulerTimeLine : public TimeLine {
 public:
  SchedulerTimeLine() {}
  ~SchedulerTimeLine() {}
  bool RemoveSingularEvent(EventType tp);
  bool RemoveMultipleEvent(EventType tp);
 private:
};

// for trafficGen
class TrafficGenTimeLine : public TimeLine {
 public:
  TrafficGenTimeLine() {}
  ~TrafficGenTimeLine() {}
 private:
};

// for simulator
class Simulator : public TimeLine {
 public:
  Simulator();
  ~Simulator();
  void UpdateSchedulerAlarm(EventNotifyScheduler *);
  bool InstallScheduler(const std::string &schedulerName);
  bool InstallTrafficGen(std::string trafficProducerName, DbLogger *db_logger);
  void UpdateTrafficAlarm(Event *);
  void Run();
  // Used for testing.
  double GetTotalCCT() { return m_trafficPtr->m_totalCCT; }
  map<int, double> GetAllCCT() { return m_trafficPtr->jobid_to_CCT_; }
  vector<Coflow *> &GetSavedCoflow() { return m_trafficPtr->coflows_saved_; }
 private:
  Scheduler *m_schedulerPtr;
  TrafficGen *m_trafficPtr;
  double m_currentTime;

  void DoNotifyAddFlows();
  void DoNotifyAddCoflows();

  void DoNotifyTrafficFinish();
};

#endif /* EVENTS_H */
