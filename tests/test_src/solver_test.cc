//
// Created by Xin Sunny Huang on 7/21/17.
//

#include "ximulator_test_base.h"
#include "src/solver_infocom.h"

typedef std::string basicString;
class SolverTest : public TrafficGeneratorTest {
 protected:
  virtual void TearDown() {}
  virtual void SetUp() { TrafficGeneratorTest::SetUp(); }
};

TEST_F(SolverTest, Infocom_OneCoflow) {
  vector<Coflow*> coflows;
  LoadAllCoflows(coflows);
  long ONE_GIGA_BPS = (long) 1e9;
  vector<Scheduler*> schedulers(
      {new SchedulerVarysImpl(ONE_GIGA_BPS / 3 * 1),
       new SchedulerVarysImpl(ONE_GIGA_BPS / 3 * 2)});
  SolverInfocom solver_infocom(schedulers, /*debug_level=*/0);
  for (Coflow* coflow : coflows) {
    map<pair<Scheduler*, int>, long> no_src_reserved_bps,
        no_dst_reserved_bps;
    double cct = solver_infocom.MinCCT(
        coflow, no_src_reserved_bps, no_dst_reserved_bps);
    cout << coflow->GetName() << " MinCCT_cct = " << cct << endl;
    switch (coflow->GetJobId()) {
      case 1 :EXPECT_NEAR(cct, 0.012, 1e-6);
        break;
      case 2 :EXPECT_NEAR(cct, 0.576, 1e-6);
        break;
      case 3 :EXPECT_NEAR(cct, 0.048, 1e-6);
        break;
      case 54 :EXPECT_NEAR(cct, 0.048, 1e-6);
        break;
      case 108:EXPECT_NEAR(cct, 0.288, 1e-6);
        break;
    }
  }
}

TEST_F(SolverTest, Infocom_ManyCoflow) {
  vector<Coflow*> coflows;
  LoadAllCoflows(coflows);
  long ONE_GIGA_BPS = 1e9;
  vector<Scheduler*> schedulers(
      {new SchedulerVarysImpl(ONE_GIGA_BPS / 3 * 1),
       new SchedulerVarysImpl(ONE_GIGA_BPS / 3 * 2)});
  SolverInfocom solver_infocom(schedulers);
  map<long, long> rates_1, rates_2;
  solver_infocom.ComputeRouteAndRate(coflows, &rates_1);
  solver_infocom.ComputeRouteAndRate(coflows, &rates_2);
  for (const auto& id_rate_pair: rates_1) {
    int flow_id = id_rate_pair.first;
    EXPECT_EQ(rates_1[flow_id], rates_2[flow_id]);
  }
}

TEST_F(SolverTest, Infocom_Debug) {
  vector<Coflow*> coflows;
  coflows.push_back(GenerateCoflow(/*time=*/0, /*coflow_id=*/0, /*num_map=*/1,
      /*num_red=*/3,/*info=*/"11#22:1.0,33:1.0,44:1.0",
      /*do_perturb=*/false,  /*avg_size=*/false));
  coflows.push_back(GenerateCoflow(/*time=*/0, /*coflow_id=*/1, /*num_map=*/1,
      /*num_red=*/3,/*info=*/"55#66:1.0,77:1.0,88:1.0",
      /*do_perturb=*/false,  /*avg_size=*/false));

  long ONE_GIGA_BPS = 1e9;
  for (Flow* flow: *coflows[0]->GetFlows()) {
    flow->SetRate(ONE_GIGA_BPS, 0L);
    flow->Transmit(0, flow->GetBitsLeft() / ONE_GIGA_BPS);
  }

  vector<Scheduler*> schedulers({new SchedulerVarysImpl(ONE_GIGA_BPS)});
  SolverInfocom solver_infocom(schedulers);
  map<long, long> rate;
  solver_infocom.ComputeRouteAndRate(coflows, &rate);

  for (const auto& flowid_rate_pair: rate) {
    cout << flowid_rate_pair.first << " " << flowid_rate_pair.second << endl;
  }
}

TEST_F(SolverTest, Varys_ManyCoflow) {

  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_5coflows_5nodes.txt";
  // disable db logging.
  traffic_generator_.reset(new TGTraceFB(nullptr/*db_logger*/));

  vector<Coflow*> coflows;
  LoadAllCoflows(coflows);
  long ONE_GIGA_BPS = 1e9;

  SchedulerVarysImpl varys;
  map<long, long> rates;
  varys.RateControlVarysImpl(coflows, rates, ONE_GIGA_BPS);

  for (Coflow* coflow : coflows) {
    for (Flow* flow: *coflow->GetFlows()) {
      long rate = rates[flow->GetFlowId()];
      basicString flow_name = std::to_string(coflow->GetJobId()) + ":"
          + std::to_string(flow->GetSrc()) + "->"
          + std::to_string(flow->GetDest());
      if (flow_name == "1:2->0" || flow_name == "5:4->1") {
        EXPECT_EQ(rate, 1000000000);
      } else if (flow_name == "4:0->2") {
        EXPECT_EQ(rate, 705882354);
      } else if (flow_name == "4:0->3") {
        EXPECT_EQ(rate, 152941176);
      } else if (flow_name == "4:0->4") {
        EXPECT_EQ(rate, 141176470);
      } else if (flow_name == "2:4->0"
          || flow_name == "3:3->0"
          || flow_name == "4:0->1") {
        EXPECT_EQ(rate, 0);
      } else {
        cerr << "Missing for " << flow_name << endl;
      }
      cout << coflow->GetName() << " " << flow->toString() << " "
           << rates[flow->GetFlowId()] << endl;
    }
  }
}

TEST_F(SolverTest, Weaver_ManyCoflow) {
  // disable db logging.
  TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR_ + "test_3coflows_150nodes.txt";
  traffic_generator_.reset(new TGTraceFB(nullptr/*db_logger*/));
  vector<Coflow*> coflows;
  LoadAllCoflows(coflows);

  std::string scheduler_name = "weaverSortedFlowDec_20varys_80varys";
  std::vector<Scheduler*>
      schedulers = SchedulerFactory::GenerateChildrenSchedulers(scheduler_name);
  std::unique_ptr<SchedulerWeaver> weaver_scheduler
      (SchedulerWeaver::Factory(scheduler_name, schedulers));
  std::map<Scheduler*, std::vector<Coflow*>*> scheduler_to_children_coflows;
  for (Scheduler* scheduler : schedulers) {
    scheduler_to_children_coflows[scheduler] = new std::vector<Coflow*>();
  }
  weaver_scheduler->AssignCoflowsToSchedulers(
      coflows, schedulers, &scheduler_to_children_coflows);

  //  for (Coflow *coflow : coflows) {
  //    for (Flow *flow: *coflow->GetFlows()) {
  //      cout << coflow->GetName() << " " << flow->toString() << " assigned to "
  //           << flow->assigned_scheduler_name_ << endl;
  //    }
  //  }

  map<long, long> all_rates;
  SchedulerVarysImpl varys;
  for (const auto& scheduler_coflows: scheduler_to_children_coflows) {
    map<long, long> rates;
    varys.RateControlVarysImpl(*scheduler_coflows.second, rates,
                               scheduler_coflows.first->SCHEDULER_LINK_RATE_BPS_);
    all_rates.insert(rates.begin(), rates.end());

    //    cout << "\nResults for RateControlVarysImpl() for "
    //         << scheduler_coflows.first->SCHEDULER_LINK_RATE_BPS_ << endl;
    //    for (Coflow *child: *scheduler_coflows.second) {
    //      for (Flow *flow: *child->GetFlows()) {
    //        cout << child->GetName() << " " << flow->toString()
    //             << " assigned rate " << rates[flow->GetFlowId()] << " bps\n";
    //      }
    //    }
  }

  for (Coflow* coflow : coflows) {
    double total_bps = 0.0;
    for (Flow* flow: *coflow->GetFlows()) {
      total_bps += double(all_rates[flow->GetFlowId()]);
    }
    cout << coflow->GetName() << " total_bps=" << total_bps << endl;
    if (coflow->GetJobId() == 21) {
      EXPECT_EQ(total_bps, 8e+08);
    } else if (coflow->GetJobId() == 54) {
      EXPECT_EQ(total_bps, 2e+09);
    } else if (coflow->GetJobId() == 108) {
      EXPECT_EQ(total_bps, 4e+09);
    }
  }

}

TEST_F(SolverTest, Weaver_Example) {

  double dummy_start_time = 0;
  Coflow coflow(dummy_start_time);
  coflow.AddFlow(new Flow(dummy_start_time, 1, 101, 90));
  coflow.AddFlow(new Flow(dummy_start_time, 2, 101, 90));
  coflow.AddFlow(new Flow(dummy_start_time, 3, 101, 90));
  coflow.AddFlow(new Flow(dummy_start_time, 1, 102, 10));
  coflow.AddFlow(new Flow(dummy_start_time, 1, 103, 10));
  coflow.AddFlow(new Flow(dummy_start_time, 2, 103, 5));
  coflow.AddFlow(new Flow(dummy_start_time, 3, 103, 5));
  std::vector<Coflow*> coflows;
  coflows.push_back(&coflow);
  std::string scheduler_name = "weaverSortedFlowDec_20varys_80varys";
  std::vector<Scheduler*>
      schedulers = SchedulerFactory::GenerateChildrenSchedulers(scheduler_name);
  std::unique_ptr<SchedulerWeaver> weaver_scheduler
      (SchedulerWeaver::Factory(scheduler_name, schedulers));
  weaver_scheduler->debug_level_ = 10;
  std::map<Scheduler*, std::vector<Coflow*>*> scheduler_to_children_coflows;
  for (Scheduler* scheduler : schedulers) {
    scheduler_to_children_coflows[scheduler] = new std::vector<Coflow*>();
  }
  weaver_scheduler->AssignCoflowsToSchedulers(
      coflows, schedulers, &scheduler_to_children_coflows);
  for (Coflow* coflow : coflows) {
    for (Flow* flow: *coflow->GetFlows()) {
      cout << coflow->GetName() << " " << flow->toString() << " assigned to "
           << flow->assigned_scheduler_name_ << endl;
    }
  }
}

TEST_F(SolverTest, Weaver_Incast) {

  double dummy_start_time = 0;
  Coflow coflow(dummy_start_time);
  coflow.AddFlow(new Flow(dummy_start_time, 1, 104, 1));
  coflow.AddFlow(new Flow(dummy_start_time, 2, 104, 1));
  coflow.AddFlow(new Flow(dummy_start_time, 3, 104, 1));
  std::vector<Coflow*> coflows;
  coflows.push_back(&coflow);
  std::string scheduler_name = "weaverSortedFlowDec_20varys_80varys";
  std::vector<Scheduler*>
      schedulers = SchedulerFactory::GenerateChildrenSchedulers(scheduler_name);
  std::unique_ptr<SchedulerWeaver> weaver_scheduler
      (SchedulerWeaver::Factory(scheduler_name, schedulers));
  weaver_scheduler->debug_level_ = 10;
  std::map<Scheduler*, std::vector<Coflow*>*> scheduler_to_children_coflows;
  for (Scheduler* scheduler : schedulers) {
    scheduler_to_children_coflows[scheduler] = new std::vector<Coflow*>();
  }
  weaver_scheduler->AssignCoflowsToSchedulers(
      coflows, schedulers, &scheduler_to_children_coflows);
  for (Coflow* coflow : coflows) {
    for (Flow* flow: *coflow->GetFlows()) {
      cout << coflow->GetName() << " " << flow->toString() << " assigned to "
           << flow->assigned_scheduler_name_ << endl;
    }
  }
}