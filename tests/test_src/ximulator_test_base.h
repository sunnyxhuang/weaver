//
// Created by Xin Sunny Huang on 3/14/17.
//

#ifndef XIMULATOR_XIMULATOR_TEST_BASE_H_H
#define XIMULATOR_XIMULATOR_TEST_BASE_H_H

#include "gtest/gtest.h"
#include "src/traffic_generator.h"
#include "src/db_logger.h"

class Coflow;

class XimulatorTestBase : public ::testing::Test {
 protected:
  XimulatorTestBase() : db_logger_("test") {

    string MAC_BASE_DIR = "../../../";
    string LINUX_BASE_DIR = "../";
    string BASE_DIR = IsOnApple() ? MAC_BASE_DIR : LINUX_BASE_DIR;

    string TEST_DATA_DIR = BASE_DIR + "tests/test_data/";
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR + "test_trace.txt";
    CCT_AUDIT_FILE_NAME = TEST_DATA_DIR + "audit_cct.txt";
    FCT_AUDIT_FILE_NAME = TEST_DATA_DIR + "audit_fct.txt";
    COMPTIME_AUDIT_FILE_NAME = TEST_DATA_DIR + "audit_comp.txt";

    TEST_DATA_DIR_ = TEST_DATA_DIR;
  }
  string TEST_DATA_DIR_;
  // db_logger_ may be shared by many many test and writes junks into the db.
  DbLogger db_logger_;
};

class TrafficGeneratorTest : public XimulatorTestBase {
 protected:
  virtual void TearDown() {}

  virtual void SetUp() {
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace.txt";
    // disable db logging.
    traffic_generator_.reset(new TGTraceFB(nullptr/*db_logger*/));
  }

  void LoadAllCoflows(vector<Coflow*>& load_to_me) {
    vector<JobDesc*> new_jobs = traffic_generator_->ReadJobs();
    while (!new_jobs.empty()) {
      for (JobDesc* job : new_jobs) {
        load_to_me.push_back(job->m_coflow);
        // deleting a job would NOT delete the coflow pointer insides.
        delete job;
      }
      new_jobs = traffic_generator_->ReadJobs();
    }
  }

  Coflow* GenerateCoflow(double time, int coflow_id, int num_map, int num_red,
                         string cfInfo, bool do_perturb, bool avg_size) {
    return traffic_generator_->CreateCoflowPtrFromString(
        time, coflow_id, num_map, num_red, cfInfo, do_perturb, avg_size);
  }
  std::unique_ptr<TGTraceFB> traffic_generator_;
};

#endif //XIMULATOR_XIMULATOR_TEST_BASE_H_H
