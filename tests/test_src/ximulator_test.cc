//
// Created by Xin Sunny Huang on 10/22/16.
//

#include "gtest/gtest.h"
#include "src/events.h"
#include "ximulator_test_base.h"

class XimulatorTest : public XimulatorTestBase {
 protected:
  virtual void TearDown() {
  }

  virtual void SetUp() {
    ELEC_BPS = 1e9;
    DEBUG_LEVEL = 0;
    TEST_ONLY_SAVE_COFLOW_AFTER_FINISH = false;
    ENABLE_PERTURB_IN_PLAY = true;
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace.txt";
    ximulator_.reset(new Simulator());
  }

  std::unique_ptr<Simulator> ximulator_;
};


TEST_F(XimulatorTest, VarysOnInterCoflow) {
  ximulator_->InstallScheduler("varysImpl");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? 2.664297 : 2.640789, 1e-6);
}

TEST_F(XimulatorTest, VarysOnInterCoflow_trace2) {
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace_2.txt";
  ximulator_->InstallScheduler("varysImpl");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 2.344487, 1e-6);
}

TEST_F(XimulatorTest, AaloOnInterCoflow) {
  ximulator_->InstallScheduler("aaloImpl");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? 3.023155 : 3.004975, 1e-6);
}


