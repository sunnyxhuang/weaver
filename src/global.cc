//
//  global.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 10/30/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#include "global.h"
#include "util.h"

// Some useful constant/scaler
//const double ONE_GIGA_DOUBLE = 1.0e9;
const long ONE_GIGA_LONG = 1e9;
// const long TEN_GIGA_LONG = 10 * 1e9;

long DEFAULT_LINK_RATE_BPS = ONE_GIGA_LONG / NUM_LINK_PER_RACK;
long ELEC_BPS = ONE_GIGA_LONG / NUM_LINK_PER_RACK; //1G = 1000000000

// If true, we assume the set of machines behind in/out ports are different.
// Thus, all traffic must traverse the network, even the indexes of src and
// dst are the same.
bool REMOTE_IN_OUT_PORTS = false;

// used by the scheduler to hack the simulation if needed.
bool SPEEDUP_1BY1_TRAFFIC_IN_SCHEDULER = false;
bool DEADLINE_MODE = false;
double DEADLINE_ERROR_TOLERANCE = 0.0001;

bool ENABLE_PERTURB_IN_PLAY = false;
// valid if ENABLE_PERTURB_IN_PLAY = false;
// all flows to the same reducer will be equally
// distributed for all mappers.
bool EQUAL_FLOW_TO_SAME_REDUCER = false;

// used to initialized end time.
double INVALID_TIME = -1.0;

// if true, log down comp time stat.
bool LOG_COMP_STAT = false;

// default num of racks used in tms to determine
// the bound for random selected rack to fill demand
int NUM_RACKS = 150;
//  NUM_LINK_PER_RACK circuits are connected for a rack;
//  rack_number = circuit_number mod NUM_RACKS
int NUM_LINK_PER_RACK = 1;

// used by traffic generator.
// which indicates max number of coflows supported per run.
// used to initialized the random seed for each coflow.
int PERTURB_SEED_NUM = 600;

// all flow byte sizes are multiplied by TRAFFIC_SIZE_INFLATE.
double TRAFFIC_SIZE_INFLATE = 1;

// the inter-arrival time of traffic is multipled by TRAFFIC_ARRIVAL_SPEEDUP;
// TRAFFIC_ARRIVAL_SPEEDUP < 1 means traffic arrives faster.
double TRAFFIC_ARRIVAL_SPEEDUP = 1;

bool TEST_ONLY_SAVE_COFLOW_AFTER_FINISH = false;

string MAC_BASE_DIR = "../../";
string LINUX_BASE_DIR = "../";
string BASE_DIR = IsOnApple() ? MAC_BASE_DIR : LINUX_BASE_DIR;

string TRAFFIC_TRACE_FILE_NAME = BASE_DIR + "trace/fbtrace-1hr.txt";

string RESULTS_DIR = "results/";
string CCT_AUDIT_FILE_NAME = BASE_DIR + RESULTS_DIR + "audit_cct.txt";
string FCT_AUDIT_FILE_NAME = BASE_DIR + RESULTS_DIR + "audit_fct.txt";
string COMPTIME_AUDIT_FILE_NAME = BASE_DIR + RESULTS_DIR + "audit_comp.txt";

bool ZERO_COMP_TIME = true;
// for aalo.
int AALO_Q_NUM = 10;
double AALO_INIT_Q_HEIGHT = 10.0 * 1000000; // 10MB
double AALO_Q_HEIGHT_MULTI = 10.0;

// a flag to indicated this coflow has inf large alpha (expected cct).
double CF_DEAD_ALPHA_SIGN = -1.0;
// consider online alpha to be infinite if longer than this cutoff,
// so that the coflow may be skipped and enter the so-called "fair-share" trick.
double ONLINE_ALPHA_CUTOFF = 10000000; // 1000000000

// by default 0 - no debug string
// more debug string with higher DEBUG_LEVEL
int DEBUG_LEVEL = 0;

// if true, output circuits to connectivity file.
bool DUMP_CIRCUIT_TO_CONNECTIVITY_FILE = false;

// if true, ignore switching circuit set warning; false by default.
bool IGNORE_WARNING_SWITCHING_CIRCUIT_SET = false;

const int FLOAT_TIME_WIDTH = 10;
