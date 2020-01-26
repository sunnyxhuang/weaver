//
// Created by Xin Sunny Huang on 10/6/17.
//

#include <iomanip>

#include "events.h"
#include "global.h"
#include "scheduler.h"
#include "solver_infocom.h"

void SchedulerInfocom::Schedule(void) {
  //  cout << fixed << setw(FLOAT_TIME_WIDTH)
  //       << m_currentTime << "s "
  //       << "[SchedulerInfocom::schedule] Infocom scheduling START" << endl;

  // STEP 1: Initialize next rate for all flows to (0,0)
  m_nextElecRate.clear();

  // STEP 2: Perform rate control, as well as routing if needed.
  SolverInfocom solver_infocom(schedulers_);
  solver_infocom.ComputeRouteAndRate(m_coflowPtrVector, &m_nextElecRate);

  cout << fixed << setw(FLOAT_TIME_WIDTH)
       << m_currentTime << "s "
       << "[SchedulerInfocom::schedule] Infocom scheduling END with rates for "
       << m_nextElecRate.size() << " flows in " << m_coflowPtrVector.size()
       << " remaining coflows." << endl;

  m_myTimeLine->RemoveSingularEvent(APPLY_NEW_SCHEDULE);
  Event *applyScheduleEventPtr = new Event(APPLY_NEW_SCHEDULE, m_currentTime);
  m_myTimeLine->AddEvent(applyScheduleEventPtr);
}