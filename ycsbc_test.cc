#include "ycsbc.h"

#include <iostream>
#include <mutex>
#include <string>

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

using std::cout;
using std::endl;

namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "A new thread begins working." << endl;
  }

  seastar::future<int> Read(const std::string &table, const std::string &key,
                            const std::vector<std::string> *fields,
                            std::vector<KVPair> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "READ " << table << ' ' << key;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout << " < all fields >" << endl;
    }
    return seastar::make_ready_future<int>(0);
  }

  seastar::future<int> Scan(const std::string &table, const std::string &key,
                            int len, const std::vector<std::string> *fields,
                            std::vector<std::vector<KVPair>> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "SCAN " << table << ' ' << key << " " << len;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout << " < all fields >" << endl;
    }
    return seastar::make_ready_future<int>(0);
  }

  seastar::future<int> MultiRead(const std::string &table,
                                 const std::vector<std::string> &keys,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<KVPair>> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "MultiGet " << table << ' ';
    cout << "keys:\t[ ";
    for (auto &key : keys) {
      cout << key << ' ';
    }
    cout << "]" << endl;
    if (fields) {
      cout << "fields:\t[ ";
      // for (auto f : *fields) {
      //   cout << f << ' ';
      // }
      cout << ']' << endl;
    } else {
      cout << " < all fields >" << endl;
    }
    return seastar::make_ready_future<int>(0);
  }

  seastar::future<int> Update(const std::string &table, const std::string &key,
                              std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "UPDATE " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;
    return seastar::make_ready_future<int>(0);
  }

  seastar::future<int> Insert(const std::string &table, const std::string &key,
                              std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "INSERT " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;
    return seastar::make_ready_future<int>(0);
  }

  seastar::future<int> Delete(const std::string &table,
                              const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "DELETE " << table << ' ' << key << endl;
    return seastar::make_ready_future<int>(0);
  }

 private:
  std::mutex mutex_;
};

}  // namespace ycsbc

int main(int argc, char *argv[]) {

  using namespace std::string_literals;

  std::vector<char *> seastar_args, ycsbc_args;

  seastar_args.push_back(argv[0]);
  ycsbc_args.push_back(argv[0]);

  {
    bool sep_met = false;

    for (int i = 1; i < argc; i++)
    {
      if (argv[i] == "--"s) {
        sep_met = true;
      }
      else if (sep_met) {
        ycsbc_args.push_back(argv[i]);
      }
      else {
        seastar_args.push_back(argv[i]);
      }
    }

    if (!sep_met) {
      std::cerr << "Usage: " << argv[0] << " <seastar args> -- <ycsbc args>" << std::endl;
      return 1;
    }
  }

  seastar::app_template app;
  app.run(seastar_args.size(), seastar_args.data(), [ycsbc_args] {
    return seastar::async([ycsbc_args]() mutable -> void {
      ycsbc::DB *basic_db = new ycsbc::BasicDB();
      ycsbc::RunBench(ycsbc_args.size(), const_cast<const char**>(ycsbc_args.data()), basic_db);
    });
  });
}
