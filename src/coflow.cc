//
//  coflow.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 9/21/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#include "coflow.h"
#include "global.h"

#include <algorithm> // stable_sort
#include <ctgmath>
#include <iomanip> // setw
#include <sstream>

using namespace std;

long Flow::s_flowIdTracker = 0;

Flow::Flow(double startTime, int src, int dest, long sizeInByte) {
  m_flowId = s_flowIdTracker++;
  m_startTime = startTime;
  m_endTime = INVALID_TIME;
  m_src = src;
  m_dest = dest;

  m_thruOptic = false;
  m_sizeInBit = sizeInByte * 8;
  m_bitsLeft = m_sizeInBit;
  m_elecBps = 0.0;
  m_optcBps = 0.0;

  bits_thru_side_ = 0;
  bits_thru_main_ = 0;

  parent_coflow_ = nullptr;
  assigned_scheduler_ = nullptr;
}

Flow::~Flow() {
  //cout << "Flow destructor called." << endl;
  /*do nothing */
}

void Flow::SetRate(long elecBps, long optcBps) {
  m_elecBps = elecBps;
  m_optcBps = optcBps;
}

bool Flow::isRawFlow() {
  if (m_bitsLeft != m_sizeInBit
      || m_elecBps != 0
      || m_optcBps != 0) {
    return false;
  }
  return true;
}

// return bitsLeft
// Require endTime > startTime
long Flow::Transmit(double startTime, double endTime) {
  long tBits = 0;
  long bps = 0;
  if (m_thruOptic) {
    tBits = ceil((endTime - startTime) * m_optcBps);
    bps = m_optcBps;
  } else {
    tBits = ceil((endTime - startTime) * m_elecBps);
    bps = m_elecBps;
  }
  long tBitsValid = tBits < m_bitsLeft ? tBits : m_bitsLeft;
  m_bitsLeft -= tBitsValid;

  //debug msg
  if (DEBUG_LEVEL >= 15) {
    cout << string(FLOAT_TIME_WIDTH + 2, ' ')
         /* make room for time in other lines */
         << "[Flow::Transmit] Flow [" << m_flowId << "] "
         << m_src << "->" << m_dest
         << " TX " << tBitsValid << " bits in"
         << "(" << startTime << ", " << endTime << ")s "
         << "rate " << bps << " bps "
         << m_bitsLeft << " bits_left" << endl;
  }

  return m_bitsLeft;
}

long Flow::TxLocal() {
  if (!REMOTE_IN_OUT_PORTS && m_src == m_dest) {
    m_bitsLeft = 0;
  }
  return m_bitsLeft;
}

long Flow::TxSalvage() {
  long bps = 0;
  if (m_thruOptic) {
    bps = m_optcBps;
  } else {
    bps = m_elecBps;
  }
  if (bps > 0 && m_bitsLeft <= 10) {
    m_bitsLeft = 0;
  }
  return m_bitsLeft;
}

bool Flow::HasDemand() {
  return REMOTE_IN_OUT_PORTS ?
         GetBitsLeft() > 0 :
         (m_src != m_dest && GetBitsLeft() > 0);
}

string Flow::toString() {
  std::stringstream ss;
  ss << "Flow-[" << m_flowId << ", "
     << m_src << "->" << m_dest << " "
     << m_bitsLeft << " bits] ";
  return ss.str();
}

void Coflow::Print() {
  for (Flow *flow: flows_) {
    if (flow->GetBitsLeft() <= 0) {
      continue;
    }
    cout << "            " /* make room for time in other lines */
         << "[Coflow::Print] Flow Id " << flow->GetFlowId() << " "
         << flow->GetSrc() << "=>" << flow->GetDest() << " "
         << flow->GetBitsLeft() << " bits_left "
         << flow->GetElecRate() << " bps_elec "
         << flow->GetOptcRate() << " bps_optc " << endl;
  }
  cout << string(FLOAT_TIME_WIDTH + 2,
                 ' ') /* make room for time in other lines */
       << "[Coflow::Print] CalcAlpha = " << CalcAlpha() << endl;
}

string Coflow::toString() {
  std::stringstream ss;
  ss << "Coflow-[" << GetName() << ", "
     << m_startTime << "+"
     << GetMaxOptimalWorkSpanInSeconds() << ", "
     << "a = " << GetAlpha() << ", "
     << setw(5) << m_nFlowsCompleted << " / "
     << setw(5) << m_nFlows << " flows] ";
  return ss.str();
}

int Coflow::s_coflowIdTracker = 0;

Coflow::Coflow(double startTime) {
  m_job_id = -1;
  m_coflowId = s_coflowIdTracker++;
  m_startTime = startTime;
  m_nFlows = 0;
  m_nFlowsCompleted = 0;
  //m_finalFlowAdded = false;
  flows_ = vector<Flow *>();
  m_alpha = -1;
  m_coflow_size_in_bytes = 0.0;
  m_coflow_sent_bytes = 0.0;
  //
  m_endTime = INVALID_TIME;
  m_deadline_duration = 0.0;
  m_is_rejected = false;

  has_route_ = false;
}

Coflow::~Coflow() {
  //  cout << "Coflow destructor called. \n";
  //delete all flows within the coflow
  for (Flow *f : flows_) {
    if (f) {
      // cout << " Deleting flow # " << f->GetFlowId() << endl;
      delete f;
    }
  }
  flows_.clear();
}

bool Coflow::NumFlowFinishInc() {
  if (m_nFlowsCompleted < m_nFlows) {
    m_nFlowsCompleted++;
    return true;
  }
  return false;
}

bool Coflow::isRawCoflow() {
  for (Flow *flow: flows_) {
    if (!flow->isRawFlow()) {
      return false;
    }
  }
  return true;
}

void
Coflow::AddFlow(Flow *fp) {
  if (!fp) return;
  flows_.push_back(fp);
  m_nFlows++;

  // do some coflow profiling.
  // by default, use 1G to estimate.
  if (!REMOTE_IN_OUT_PORTS && fp->GetSrc() == fp->GetDest()) return;

  double time = fp->GetSizeInBit() / (double) DEFAULT_LINK_RATE_BPS;
  MapWithInc(m_src_time, fp->GetSrc(), time);
  MapWithInc(m_dst_time, fp->GetDest(), time);
  long bits = fp->GetSizeInBit();
  MapWithInc(m_src_bits, fp->GetSrc(), bits);
  MapWithInc(m_dst_bits, fp->GetDest(), bits);
  m_coflow_size_in_bytes += (bits / 8.0);

}

long
Coflow::GetMaxPortLoadInBits() {
  double max_src = MaxMap(m_src_bits);
  double max_dst = MaxMap(m_dst_bits);
  return max_src > max_dst ? max_src : max_dst;
}

double Coflow::GetMaxPortLoadInSec() {
  return GetMaxPortLoadInBits() / (double) DEFAULT_LINK_RATE_BPS;
}

double Coflow::GetMaxPortLoadInSeconds() {
  long bits = GetLoadOnMaxOptimalWorkSpanInBits();
  return bits / (double) DEFAULT_LINK_RATE_BPS;
}

double Coflow::GetMaxOptimalWorkSpanInSeconds() {
  double max_src = MaxMap(m_src_time);
  double max_dst = MaxMap(m_dst_time);
  return max_src > max_dst ? max_src : max_dst;
}

//return payload, in bits, on the src/dst port
// with the max optimal work span.
long Coflow::GetLoadOnMaxOptimalWorkSpanInBits() {
  int src = -1;
  int dst = -1;
  GetPortOnMaxOptimalWorkSpan(src, dst);
  if ((src <= -1) != (dst <= -1)) {
    // solid pair : either one is valid.
    return GetLoadOnPortInBits(src, dst);
  }
  cout << __func__ << ": port pair invalid" << endl;
  return -1;
}

//return port with max optimal-work-span
void Coflow::GetPortOnMaxOptimalWorkSpan(int &src, int &dst) {
  double max_src = MaxMap(m_src_time);
  double max_dst = MaxMap(m_dst_time);
  if (max_src > max_dst) {
    // max optimal-work-span on src.
    src = Key2MaxPositiveMap(m_src_time);
    dst = -1;
    //    cout << "max optimal work span on src " << src << " time " << max_src
    //         << endl;
  } else {
    // max optimal-work-span on dst.
    src = -1;
    dst = Key2MaxPositiveMap(m_dst_time);
    //    cout << "max optimal work span on dst " << dst << " time " << max_dst
    //         << endl;
  }
}

long Coflow::GetLoadOnPortInBits(int src, int dst) {
  if ((src <= -1) != (dst <= -1)) {
    // solid pair : either one is valid.
    return dst <= -1 ?
           MapWithDef(m_src_bits, src, (long) 0) : MapWithDef(m_dst_bits,
                                                              dst,
                                                              (long) 0);;
  }
  cout << __func__ << ": port pair invalid" << endl;
  return -1;
}

double Coflow::GetOptimalWorkSpanOnPortInSeconds(int src, int dst) {
  if ((src <= -1) != (dst <= -1)) {
    // solid pair : either one is valid.
    return dst <= -1 ?
           MapWithDef(m_src_time, src, 0.0) : MapWithDef(m_dst_time, dst, 0.0);;
  }
  cout << __func__ << ": port pair invalid" << endl;
  return -1;
}

long Coflow::CalcTotalBitsLeft() {
  m_bits_left = 0;
  for (Flow *flow : flows_) {
    m_bits_left += flow->GetBitsLeft();
  }
  return m_bits_left;
}

// the higher, the less prioritized
long Coflow::CalcAlpha() {
  long maxPortSumBits = 0;
  map<int, long> sBits;
  map<int, long> rBits;
  for (Flow *flow :flows_) {
    MapWithInc(sBits, flow->GetSrc(), flow->GetBitsLeft());
    MapWithInc(rBits, flow->GetDest(), flow->GetBitsLeft());
  }

  long sBitsMax = MaxMap(sBits);
  long rBitsMax = MaxMap(rBits);

  maxPortSumBits = sBitsMax > rBitsMax ? sBitsMax : rBitsMax;

  m_alpha = maxPortSumBits;
  return maxPortSumBits;
}

bool coflowCompAlpha(Coflow *l, Coflow *r) {
  return l->GetAlpha() <= r->GetAlpha();
}

bool flowCompBitsLeft(Flow *l, Flow *r) {
  return l->GetBitsLeft() <= r->GetBitsLeft();
}

void CalAlphaAndSortCoflowsInPlace(vector<Coflow *> &coflows) {
  for (Coflow *coflow :coflows) {
    coflow->CalcAlpha();
  }
  std::stable_sort(coflows.begin(), coflows.end(), coflowCompAlpha);
}
