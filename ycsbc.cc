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

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>

using namespace std;

namespace ycsbc {
void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                   bool is_loading, vector<double> *latency) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  utils::Timer<double> timer_us;
  int current_ops;

  int oks = 0;

  std::vector<seastar::future<bool>> futs;

  for (int i = 0; i < num_ops; ++i) {
    current_ops = 0;

    // TODO: Do not use seastar::thread here
    futs.push_back(seastar::async([is_loading, &client, latency]() {
      bool ret = false;
      utils::Timer<double> timer_us;
      timer_us.Start();
      if (is_loading) {
        ret = client.DoInsert().get();
      } else {
        ret = client.DoTransaction().get();
      }
      double t = timer_us.End();
      if (latency) latency->push_back(t);
      return ret;
    }));
  }

  for (auto &fut : futs) oks += fut.get();

  db->Close();
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
    } else if (strcmp(argv[argindex], "-bs") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("fieldlength", argv[argindex]);
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
  cout
      << "   -operations operation_counts: amount of operations to be performed"
      << endl;
  cout << "   -bs value_size: size of a value" << endl;
  cout << "   --no-init: bypass the initialization of data" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void RunBench(int argc, const char *argv[], DB *db) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);
  vector<seastar::future<int>> actual_ops;
  int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
  int sum = 0;

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  const bool init_data = stoi(props.GetProperty("init_data", "1"));

  vector<double> total_latency;
  total_latency.reserve(total_ops);
  vector<vector<double>> thread_latency(num_threads);
  if (init_data) {  // Loads data
    std::cout << "=============================== Load Data "
                 "==============================="
              << std::endl;

    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(seastar::smp::submit_to(
          i, [db, &wl, ops = total_ops / num_threads]() {
            return seastar::async([db, &wl, ops]() {
              return DelegateClient(db, &wl, ops, true, nullptr);
            });
          }));
    }
    assert((int)actual_ops.size() == num_threads);

    for (auto &n : actual_ops) {
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
    actual_ops.emplace_back(seastar::smp::submit_to(
        i, [db, &wl, ops = total_ops / num_threads, &thread_latency, i]() {
          return seastar::async([db, &wl, ops, &thread_latency, i]() {
            return DelegateClient(db, &wl, ops, false, &thread_latency[i]);
          });
        }));
  }
  assert((int)actual_ops.size() == num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    sum += n.get();
  }
  double duration = timer.End();

  for (int t = 0; t < num_threads; t++) {
    total_latency.insert(total_latency.end(), thread_latency[t].begin(),
                         thread_latency[t].end());
  }
  size_t pos_avg = sum >> 2;
  size_t pos_99 = sum - sum / 100 - 1;
  size_t pos_999 = sum - sum / 1000 - 1;

  cout << "# Transaction throughput (KTPS)" << endl;
  cout << file_name << '\t' << num_threads << '\t';
  cout << total_ops / duration / 1000 << endl;

  cout << "# Transaction latency (ms)" << endl;
  nth_element(total_latency.begin(), total_latency.begin() + pos_avg,
              total_latency.end());
  cout << "avg latency:\t" << *(total_latency.begin() + pos_avg) * 1000 << endl;
  nth_element(total_latency.begin(), total_latency.begin() + pos_99,
              total_latency.end());
  cout << "99% tail latency:\t" << *(total_latency.begin() + pos_99) * 1000
       << endl;
  nth_element(total_latency.begin(), total_latency.begin() + pos_999,
              total_latency.end());
  cout << "99.9% tail latency:\t" << *(total_latency.begin() + pos_999) * 1000
       << endl;
}
}  // namespace ycsbc
