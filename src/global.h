//
//  global.h
//  Ximulator
//
//  Created by Xin Sunny Huang on 10/27/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#ifndef Ximulator_global_h
#define Ximulator_global_h

#include <iostream>

using namespace std;

extern long DEFAULT_LINK_RATE_BPS;
extern long ELEC_BPS;

extern bool REMOTE_IN_OUT_PORTS;

extern bool SPEEDUP_1BY1_TRAFFIC_IN_SCHEDULER;
extern bool DEADLINE_MODE;
extern double DEADLINE_ERROR_TOLERANCE;

extern bool ENABLE_LEAK_CHECK;
extern bool LEAK_CHECK_EXIT;
extern bool ENABLE_PERTURB_IN_PLAY;
extern bool EQUAL_FLOW_TO_SAME_REDUCER;

extern double INVALID_TIME;

extern bool LOG_COMP_STAT;

extern int NUM_RACKS;
extern int NUM_LINK_PER_RACK;

extern int PERTURB_SEED_NUM;

extern string TRAFFIC_TRACE_FILE_NAME;
extern string CCT_AUDIT_FILE_NAME;
extern string FCT_AUDIT_FILE_NAME;
extern string COMPTIME_AUDIT_FILE_NAME;

extern bool ZERO_COMP_TIME;

extern double TRAFFIC_SIZE_INFLATE;
extern double TRAFFIC_ARRIVAL_SPEEDUP;

extern bool TEST_ONLY_SAVE_COFLOW_AFTER_FINISH;

// for aalo.
extern int AALO_Q_NUM;
extern double AALO_INIT_Q_HEIGHT;
extern double AALO_Q_HEIGHT_MULTI;

// a flag to indicated this coflow has inf large alpha.
extern double CF_DEAD_ALPHA_SIGN;
extern double ONLINE_ALPHA_CUTOFF;

extern int DEBUG_LEVEL;

typedef enum Event_Type {
  NO_EVENT,
  SUB_JOB,                /*for TrafficGen*/
  WORKER_FINISH,
  FLOW_FINISH,            /*for Scheduler */
  COFLOW_ARRIVE,
  FLOW_ARRIVE,
  RESCHEDULE,
  ORDER_CIRCUIT,
  APPLY_CIRCUIT,
  APPLY_NEW_SCHEDULE,
  SCHEDULE_END,
  MSG_ADD_FLOWS,          /*for Simulator */
  MSG_ADD_COFLOWS,
  ALARM_TRAFFIC,
  MSG_TRAFFIC_FINISH,
  ALARM_SCHEDULER,
} EventType;

extern const int FLOAT_TIME_WIDTH;

#endif
