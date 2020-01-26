//
// Created by Xin Sunny Huang on 3/13/17.
//

#include <fstream>
#include <sstream>

#include "db_logger.h"
#include "util.h"

// static
bool DbLogger::LOG_FLOW_INFO_ = true;

void DbLogger::WriteCoflowFeatures(Coflow* coflow) {

  // read a coflow. start building up knowledge.
  double total_flow_size_Gbit = 0.0;
  double min_flow_size_Gbit = -1.0;
  double max_flow_size_Gbit = -1.0;

  int flow_num = (int) coflow->GetFlows()->size();
  for (Flow* flow : *(coflow->GetFlows())) {
    double this_flow_size_Gbit = ((double) flow->GetSizeInBit() / 1e9);
    total_flow_size_Gbit += this_flow_size_Gbit; // each flow >= 1MB
    if (min_flow_size_Gbit > this_flow_size_Gbit || min_flow_size_Gbit < 0) {
      min_flow_size_Gbit = this_flow_size_Gbit;
    }
    if (max_flow_size_Gbit < this_flow_size_Gbit || max_flow_size_Gbit < 0) {
      max_flow_size_Gbit = this_flow_size_Gbit;
    }
  }

  double avg_flow_size_Gbit = 0;
  if (flow_num > 0) {
    avg_flow_size_Gbit = total_flow_size_Gbit / (double) flow_num;
  }

  string cast_pattern = "";
  int map = coflow->GetNumMap();
  int red = coflow->GetNumRed();

  if (map == 1 && red == 1) {
    cast_pattern = "1";              // single-flow
  } else if (map > 1 && red == 1) {
    cast_pattern = "m21";            // incast
  } else if (map == 1 && red > 1) {
    cast_pattern = "12m";            // one sender, multiple receiver.
  } else if (map > 1 && red > 1) {
    cast_pattern = "m2m";           // many-to-many
  }

  string bin;
  // double lb_elec_MB = lb_elec / (double) 8.0/ 1000000;
  bin += (avg_flow_size_Gbit / 8.0 * 1e3 < 5.0) ?
         'S' : 'L'; // short if avg flow size < 5MB, long other wise
  bin += (flow_num <= 50) ? 'N' : 'W';// narrow if max flow# <= 50

  if (!has_cct_header_) {
    coflow_feature_header_ = "job_id, cast, bin, num_map, num_red, num_flow, "
                             "map_loc, red_loc, bn_load_Gbit, lb_elec, "
                             "lb_elec_Gbit, ttl_Gbit, avg_Gbit, min_Gbit, "
                             "max_Gbit";
  }
  stringstream feature;
  feature << coflow->GetJobId() << ','
          << "'" << cast_pattern << "'" << ','
          << "'" << bin << "'" << ','
          << map << ','
          << red << ','
          << flow_num << ','
          << "'" << Join(coflow->GetMapperLocations(), '_') << "'" << ','
          << "'" << Join(coflow->GetReducerLocations(), '_') << "'" << ','
          << coflow->GetMaxPortLoadInBits() / (double) 1e9 << ','// bn_load_GB
          << coflow->GetMaxPortLoadInSec() << ','                // lb_elec
          << coflow->GetMaxPortLoadInBits() / (double) 1e9 << ','// lb_elec_Gbit
          << total_flow_size_Gbit << ','
          << avg_flow_size_Gbit << ','
          << min_flow_size_Gbit << ','
          << max_flow_size_Gbit;

  coflow_features_[coflow->GetJobId()] = feature.str();
}

void DbLogger::WriteOnCoflowFinish(double finish_time, Coflow* coflow,
                                   ofstream& out) {
  double cct = (finish_time - coflow->GetStartTime());
  double elec_lb = coflow->GetMaxPortLoadInSeconds();
  double cct_over_elec = cct / elec_lb;

  double main_Gbit = 0, side_Gbit = 0;
  for (Flow* flow : *coflow->GetFlows()) {
    main_Gbit += flow->GetBitsOnMain() / (double) 1e9;
    side_Gbit += flow->GetBitsOnSide() / (double) 1e9;
  }
  map<int, double> main_src_Gbit, main_dst_Gbit, side_src_Gbit, side_dst_Gbit;
  for (Flow* flow : *coflow->GetFlows()) {
    // bn load for traffic thru elec
    main_src_Gbit[flow->GetSrc()] += flow->GetBitsOnMain() / (double) 1e9;
    main_dst_Gbit[flow->GetDest()] += flow->GetBitsOnMain() / (double) 1e9;
    // bn load for traffic thru optc
    side_src_Gbit[flow->GetSrc()] += flow->GetBitsOnSide() / (double) 1e9;
    side_dst_Gbit[flow->GetDest()] += flow->GetBitsOnSide() / (double) 1e9;
  }
  double main_bn_Gbit = max(MaxMap(main_src_Gbit), MaxMap(main_dst_Gbit));
  double side_bn_Gbit = max(MaxMap(side_src_Gbit), MaxMap(side_dst_Gbit));

  if (!has_cct_header_) {
    has_cct_header_ = true;
    out << coflow_feature_header_ << ", " << "tArr, tFin, cct, main_Gbit, "
        << "side_Gbit, main_bn_Gbit, side_bn_Gbit, r_elec" << endl;
  }
  out << coflow_features_[coflow->GetJobId()]
      << coflow->GetStartTime() << ',' << finish_time << ','
      << (cct > 0 ? to_string(cct) : "null") << ','
      << main_Gbit << ',' << side_Gbit << ','
      << main_bn_Gbit << ',' << side_bn_Gbit << ','
      << cct_over_elec << endl;
}

void DbLogger::WriteOnFlowFinish(double finish_time, Flow* flow,
                                 ofstream& out) {
  if (!LOG_FLOW_INFO_) return;
  double fct = (finish_time - flow->GetStartTime());

  if (!has_fct_header_) {
    has_fct_header_ = true;
    out << "job_id, flow_id, src, dst, flow_size_bit, tArr, tFin, fct" << endl;
  }
  out << flow->GetParentCoflow()->GetJobId() << ','
      << flow->GetFlowId() << ','
      << flow->GetSrc() << ','
      << flow->GetDest() << ','
      << flow->GetSizeInBit() << ','
      << flow->GetStartTime() << ','
      << finish_time << ','
      << fct << endl;
}