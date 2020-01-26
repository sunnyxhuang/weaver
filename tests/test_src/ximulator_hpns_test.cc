//
// Created by Xin Sunny Huang on 12/17/17.
//

#include "gtest/gtest.h"
#include "src/events.h"
#include "ximulator_test_base.h"

class XimulatorHPNsTest : public XimulatorTestBase {
 protected:
  virtual void TearDown() {
  }

  virtual void SetUp() {
    ELEC_BPS = 1e9;
    DEBUG_LEVEL = 0;
    TEST_ONLY_SAVE_COFLOW_AFTER_FINISH = false;
    ENABLE_PERTURB_IN_PLAY = false;
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace.txt";
    ximulator_.reset(new Simulator());
  }

  void VerifyInfocomWorse(int first_ratio, int second_ratio,
                          string trace_file_name) {
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + trace_file_name;
    std::string net_config = std::to_string(first_ratio) + "varys_"
        + std::to_string(second_ratio) + "varys";
    // first with weaver solver.
    ximulator_->InstallScheduler("weaver_" + net_config);
    ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
    ximulator_->Run();
    map<int, double> weaver_ccts = ximulator_->GetAllCCT();

    // then with
    SetUp();
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + trace_file_name;
    ximulator_->InstallScheduler("infocom_" + net_config);
    ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
    ximulator_->Run();
    map<int, double> infocom_ccts = ximulator_->GetAllCCT();

    EXPECT_EQ(weaver_ccts.size(), infocom_ccts.size());
    for (const auto& infocom_id_cct_pair:infocom_ccts) {
      int job_id = infocom_id_cct_pair.first;
      cout << job_id << " " << weaver_ccts[job_id] << " (?<) "
           << infocom_ccts[job_id] << endl;
      EXPECT_LE(weaver_ccts[job_id], infocom_ccts[job_id]);
    }
  }

  std::unique_ptr<Simulator> ximulator_;
};

// Run two simulations, both in single coflow mode, all sample Coflows should be
// worse under infocom solver.
// Note: this test can take a long time.
//TEST_F(XimulatorHPNsTest, InfocomWorseThanWeaverOnIntraCoflow) {
//  // int first_ratio = 10, second_ratio = 90;
//  int first_ratio = 40, second_ratio = 60;
//  string trace_file_name = "infocom_worse_than_weaver_"
//      + std::to_string(first_ratio) + "_"
//      + std::to_string(second_ratio) + ".txt";
//  VerifyInfocomWorse(first_ratio, second_ratio, trace_file_name);
//}

TEST_F(XimulatorHPNsTest, InfocomWorseThanWeaverOnExample) {
  int first_ratio = 20, second_ratio = 80;
  string trace_file_name = "infocom_worse_example.txt";
  DEBUG_LEVEL = 4;
  VerifyInfocomWorse(first_ratio, second_ratio, trace_file_name);
}

TEST_F(XimulatorHPNsTest, WeaverOnInfocomDebug) {
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "debug_infocom.txt";
  ximulator_->InstallScheduler("weaver_20varys_80varys");
  ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
  ximulator_->Run();
  map<int, double> infocom_ccts = ximulator_->GetAllCCT();
  for (const auto& infocom_cct : infocom_ccts) {
    double cct = infocom_cct.second;
    switch (infocom_cct.first) {
      case 19 :EXPECT_NEAR(cct, 1.859999, 1e-6);
        break;
      case 555 :EXPECT_NEAR(cct, 3.368949, 1e-6);
        break;
    }
  }
}

TEST_F(XimulatorHPNsTest, InfocomOnInfocomDebug) {
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "debug_infocom.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("infocom_100varys");
  ximulator_->Run();
  map<int, double> infocom_ccts = ximulator_->GetAllCCT();
  for (const auto& infocom_cct : infocom_ccts) {
    double cct = infocom_cct.second;
    switch (infocom_cct.first) {
      case 299 :EXPECT_NEAR(cct, 567.240, 1e-3);
        break;
    }
  }
}

// test variants of the weaver solver
TEST_F(XimulatorHPNsTest, WeaverSorted) {
  DEBUG_LEVEL = 4;
  ENABLE_PERTURB_IN_PLAY = true;
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "infocom_worse_example.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("weaverSortedFlowInc_70varys_30varys");
  ximulator_->Run();
  map<int, double> sorted_flow_inc_ccts = ximulator_->GetAllCCT();

  SetUp();
  DEBUG_LEVEL = 4;
  ENABLE_PERTURB_IN_PLAY = true;
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "infocom_worse_example.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("weaverSortedFlowDec_70varys_30varys");
  ximulator_->Run();
  map<int, double> sorted_flow_dec_ccts = ximulator_->GetAllCCT();

  SetUp();
  DEBUG_LEVEL = 4;
  ENABLE_PERTURB_IN_PLAY = true;
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "infocom_worse_example.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("weaverSortedSrcDstIdx_70varys_30varys");
  ximulator_->Run();
  map<int, double> sorted_idx_ccts = ximulator_->GetAllCCT();

  SetUp();
  DEBUG_LEVEL = 4;
  ENABLE_PERTURB_IN_PLAY = true;
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "infocom_worse_example.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("weaverSortedRandom_70varys_30varys");
  ximulator_->Run();
  map<int, double> sorted_random_ccts = ximulator_->GetAllCCT();

}


// Test for schedulers on scheduling single coflow.
TEST_F(XimulatorHPNsTest, WeaverOnIntraCoflow_2net) {
  ximulator_->InstallScheduler("weaver_20varys_80varys");
  ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 3.260000, 1e-6);
}

TEST_F(XimulatorHPNsTest, InfocomOnIntraCoflow) {
  ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
  ximulator_->InstallScheduler("infocom_20varys_80varys");
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 3.870000, 1e-6);
}

// Test for schedulers on scheduling multiple coflows.
TEST_F(XimulatorHPNsTest, WeaverOnInterCoflow_2net) {
  ximulator_->InstallScheduler("weaver_20varys_80varys");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 3.291000, 1e-6);
}

TEST_F(XimulatorHPNsTest, WeaverOnInterCoflow_2net_OnlineTrace2) {
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace_2.txt";
  ximulator_->InstallScheduler("weaver_20varys_80varys");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 2.980000, 1e-6);
}

TEST_F(XimulatorHPNsTest, InfocomOnInterCoflow) {
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_trace.txt";
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->InstallScheduler("infocom_20varys_80varys");
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 3.399656, 1e-6);
}

TEST_F(XimulatorHPNsTest, WeaverOnIntraCoflow_3net) {
  ximulator_->InstallScheduler("weaver_20varys_20varys_60varys");
  ximulator_->InstallTrafficGen("fb1by1", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 4.226667, 1e-6);
}

TEST_F(XimulatorHPNsTest, WeaverOnInterCoflow_3net) {
  ximulator_->InstallScheduler("weaver_20varys_20varys_60varys");
  ximulator_->InstallTrafficGen("fbplay", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(),
              REMOTE_IN_OUT_PORTS ? -1 : 4.266667, 1e-6);
}