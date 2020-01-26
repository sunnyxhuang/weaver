//
//  util.cc
//  Ximulator
//
//  Created by Xin Sunny Huang on 10/19/14.
//  Copyright (c) 2014 Xin Sunny Huang. All rights reserved.
//

#include <algorithm> // transform
#include <string>
#include <unistd.h> // get hoot name

#include "util.h"

using namespace std;

string ToLower(const string& in) {
  string result = in;
  transform(in.begin(), in.end(), result.begin(), ::tolower);
  return result;
}

bool IsOnApple() {
  size_t HOST_NAME_MAX = 100;
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, sizeof(hostname));
  std::string host = hostname;
  return (std::string::npos != std::string(hostname).find("MBP")
      || std::string::npos != std::string(hostname).find("mbp"));
}

unsigned long split(const string& txt, vector<string>& strs, char ch) {
  unsigned long pos = txt.find(ch);
  unsigned long initialPos = 0;
  strs.clear();

  // Decompose statement
  while (pos != std::string::npos) {
    strs.push_back(txt.substr(initialPos, pos - initialPos));
    initialPos = pos + 1;

    pos = txt.find(ch, initialPos);
  }

  // Add the last one
  unsigned long lastPos = pos < txt.size() ? pos : txt.size();
  strs.push_back(txt.substr(initialPos, lastPos - initialPos));

  return strs.size();
}

double secondPass(struct timeval end_time, struct timeval start_time) {
  return (double) (end_time.tv_sec - start_time.tv_sec)
      + ((double) (end_time.tv_usec - start_time.tv_usec)) / (double) 1000000;
}
 