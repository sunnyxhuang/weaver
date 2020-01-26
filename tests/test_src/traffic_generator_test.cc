//
// Created by Xin Sunny Huang on 3/5/17.
//

#include <memory>

#include "gtest/gtest.h"
#include "src/coflow.h"
#include "src/traffic_generator.h"
#include "ximulator_test_base.h"

TEST_F(TrafficGeneratorTest, TaskPlacementFromTrace) {

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow: coflows) {
    set<int> actual_mappers, actual_reducers;
    for (Flow *flow : *coflow->GetFlows()) {
      actual_mappers.insert(flow->GetSrc());
      actual_reducers.insert(flow->GetDest());
    }
    set<int> expected_mappers, expected_reducers;
    switch (coflow->GetJobId()) {
      case 2: //
        expected_mappers = set<int>({104, 132});
        expected_reducers = set<int>({140});
        break;
      case 54: //
        expected_mappers = set<int>({3, 66, 79, 121, 148});
        expected_reducers = set<int>({14, 100});
        break;
      case 108: //
        expected_mappers = set<int>({60, 89, 125, 126});
        expected_reducers = set<int>({14, 22, 31, 35, 45, 51, 54, 55, 60, 68,
                                      89, 99, 102, 115, 118});
        break;
      default:continue; // with next coflow
    }
    EXPECT_EQ(expected_mappers, actual_mappers);
    EXPECT_EQ(expected_reducers, actual_reducers);
  }
}
