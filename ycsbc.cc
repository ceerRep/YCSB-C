//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//
#include "ycsbc.h"

#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#include "core/client.h"
#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"

using namespace std;

namespace ycsbc {
void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                   bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }
  db->Close();
  // cout << "DelegateClient Finish in thread " << this_thread::get_id() <<
  // endl;
  return oks;
}

string ParseCommandLine(int argc, const char *argv[],
                        utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "--no-init") == 0) {
      props.SetProperty("init_data", "0");
      argindex++;
    } else if (strcmp(argv[argindex], "-records") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("recordcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-operations") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("operationcount", argv[argindex]);
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple "
          "files can"
       << endl;
  cout << "                   be specified, and will be processed in the order "
          "specified"
       << endl;
  cout << "   -records record_counts: amount of data to be initialized" << endl;
  cout << "   -operations operation_counts: amount of operations to be performed" << endl;
  cout << "   --no-init: bypass the initialization of data" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void RunBench(int argc, const char *argv[], DB *db) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  vector<future<int>> actual_ops;
  int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  int sum = 0;

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  const bool init_data = stoi(props.GetProperty("init_data", "1"));

  if (init_data) {  // Loads data
    std::cout << "=============================== Load Data "
                 "==============================="
              << std::endl;

    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                    total_ops / num_threads, true));
    }
    assert((int)actual_ops.size() == num_threads);

    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    cerr << "# Loading records:\t" << sum << endl;
  }

  // Peforms transactions
  std::cout << "=============================== Perform Transanction "
               "==============================="
            << std::endl;
  actual_ops.clear();
  total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  utils::Timer<double> timer;
  timer.Start();
  for (int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                  total_ops / num_threads, false));
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration = timer.End();
  cout << "# Transaction throughput (KTPS)" << endl;
  cout << file_name << '\t' << num_threads << '\t';
  cout << total_ops / duration / 1000 << endl;
}
}  // namespace ycsbc
