//
// Created by Xin Sunny Huang on 3/13/17.
//

#ifndef XIMULATOR_DB_LOGGER_H
#define XIMULATOR_DB_LOGGER_H

#include <iostream>

#include "coflow.h"

using namespace std;

class DbLogger {
 public:
  DbLogger(string table_subfix = "") : table_subfix_(table_subfix),
                                       has_cct_header_(false),
                                       has_fct_header_(false) {}
  ~DbLogger() {}

  // Write one entry for each coflow. Each entry contains metrics on coflow
  // characteristics. No simulation is needed to obtain these metrics.
  void WriteCoflowFeatures(Coflow* coflow);
  // Called when the coflow/flow is finished. Calculate some metrics on
  // scheduling performance, and log to database.
  void WriteOnCoflowFinish(double finish_time, Coflow* coflow, ofstream& out);
  void WriteOnFlowFinish(double finish_time, Flow* flow, ofstream& out);

 private:

  static bool LOG_FLOW_INFO_;

  string table_subfix_;

  bool has_fct_header_;
  bool has_cct_header_;
  string coflow_feature_header_;
  // map from coflow/job id to a string describing coflow characteristics.
  map<int, string> coflow_features_;


};

#endif //XIMULATOR_DB_LOGGER_H
