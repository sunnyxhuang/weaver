//
//  main.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/20/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
// 

#include <iostream>

#include "events.h"
#include "db_logger.h"

using namespace std;

int main(int argc, const char* argv[]) {

  // *- "weaver"  : Coflow scheduler for Heterogeneous Parallel Network (HPNs)

  // *- "prandom" : Scheduler with weighted random traffic assignment.

  // *- "infocom" : Rapier scheduler presented in INFOCOM '15. Rapier is a
  //                coflow scheduler designed for the generic topology.

  // *- "varysImpl" : varys implemention _similar_ to that on GitHub
  //                  = selfish coflow + flow based Work Conservation
  //                  Tweaked by Sunny because the performance of the original
  //                  implementation is bad.
  //  - "aaloImpl" : aalo implemented as seen in GitHub

  // For weaver/prandom/infocom, we support two forms of format to specify the
  // HPNs configurations.
  // 1. {weaver, prandom, infocom}_Nnet_{varys, aalo}_ratioA_ratioB, or
  // 2. {weaver, prandom, infocom}_ratioAscheduler_ratioBscheduler, where
  // `scheduler` is the name of BA and can be {varys, aalo}

  // For `infocom`, the BA names of {varys, aalo} have no actual effects as
  // Repier would manage bandwidth allocation.

  // An example scheduler name
  string schedulerName = "prandom_2net_varys_20_80";

  TRAFFIC_SIZE_INFLATE = 1;
  TRAFFIC_ARRIVAL_SPEEDUP = 1;

  ELEC_BPS = 1e9;

  DEBUG_LEVEL = 0;

  // trace replay:  "fbplay" | "fb1by1"
  string trafficProducerName = "fbplay";

  for (int i = 1; i < argc; i = i + 2) {
    /* We will iterate over argv[] to get the parameters stored inside.
     * Note that we're starting on 1 because we don't need to know the
     * path of the program, which is stored in argv[0] */
    if (i + 1 != argc) { // Check that we haven't finished parsing already
      string strFlag = string(argv[i]);
      if (strFlag == "-elec") {
        string content(argv[i + 1]);
        ELEC_BPS = stol(content);
      } else if (strFlag == "-s") {
        schedulerName = string(argv[i + 1]);
      } else if (strFlag == "-inflate") {
        string content(argv[i + 1]);
        TRAFFIC_SIZE_INFLATE = stod(content);
      } else if (strFlag == "-speedup") {
        string content(argv[i + 1]);
        TRAFFIC_ARRIVAL_SPEEDUP = stod(content);
      } else if (strFlag == "-traffic") {
        trafficProducerName = string(argv[i + 1]);
      } else if (strFlag == "-ftrace") {
        TRAFFIC_TRACE_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-cctaudit") {
        CCT_AUDIT_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-fctaudit") {
        FCT_AUDIT_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-compaudit") {
        COMPTIME_AUDIT_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-zc") {
        string content(argv[i + 1]);
        ZERO_COMP_TIME = (ToLower(content) == "true");
      } else {
        cout << "invalid arguments " << strFlag << " \n";
        exit(0);
      }
    }
  }

  // DEFAULT_LINK_RATE_BPS for bottle-neck calculation
  DEFAULT_LINK_RATE_BPS = ELEC_BPS;

  string db_table_subfix = schedulerName + "_" + trafficProducerName;
  if (TRAFFIC_SIZE_INFLATE > 1) {
    db_table_subfix += "_inflateX" + std::to_string((int) TRAFFIC_SIZE_INFLATE);
  }
  if (TRAFFIC_ARRIVAL_SPEEDUP > 1) {
    db_table_subfix +=
        "_speedupX" + std::to_string((int) TRAFFIC_ARRIVAL_SPEEDUP);
  }
  DbLogger db_logger(db_table_subfix);

  Simulator ximulator;
  // Some configs may be changed inside traffic generator and the scheduler.
  ximulator.InstallTrafficGen(trafficProducerName, &db_logger);
  ximulator.InstallScheduler(schedulerName);

  // Print out some important configurations.
  cout << "schedulerName = " << schedulerName << endl;
  cout << "trafficProducer = " << trafficProducerName << endl;
  cout << "TRAFFIC_SIZE_INFLATE = " << TRAFFIC_SIZE_INFLATE << endl;
  cout << "TRAFFIC_ARRIVAL_SPEEDUP = " << TRAFFIC_ARRIVAL_SPEEDUP << endl;
  cout << "ELEC_BPS = " << ELEC_BPS << endl;
  cout << "REMOTE_IN_OUT_PORTS = " << std::boolalpha << REMOTE_IN_OUT_PORTS
       << endl;
  cout << "ZERO_COMP_TIME = " << std::boolalpha << ZERO_COMP_TIME << endl;
  cout << "NUM_RACKS = " << NUM_RACKS << " * "
       << "NUM_LINK_PER_RACK = " << NUM_LINK_PER_RACK << endl;
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << "ENABLE_PERTURB_IN_PLAY = " << std::boolalpha
       << ENABLE_PERTURB_IN_PLAY << endl;
  cout << "LOG_COMP_STAT = " << std::boolalpha << LOG_COMP_STAT << endl;
  cout << " *  *  " << endl;
  cout << " *  *  " << endl;
  int file_name_cutoff = (int) TRAFFIC_TRACE_FILE_NAME.size() - 30;
  if (file_name_cutoff < 0) file_name_cutoff = 0;
  cout << "TRAFFIC_TRACE_FILE_NAME = "
       << TRAFFIC_TRACE_FILE_NAME.substr(file_name_cutoff) << endl;

  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";

  // Unless replay mode, scheduler is useless and we do not run simulation.
  ximulator.Run();

  return 0;
}


