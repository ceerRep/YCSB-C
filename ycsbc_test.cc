#include "ycsbc.h"

#include <iostream>
#include <mutex>
#include <string>

using std::cout;
using std::endl;

namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "A new thread begins working." << endl;
  }

  int Read(const std::string &table, const std::string &key,
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
    return 0;
  }

  int Scan(const std::string &table, const std::string &key, int len,
           const std::vector<std::string> *fields,
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
    return 0;
  }

  int MultiRead(const std::string &table, const std::vector<std::string> &keys,
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
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "UPDATE " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;
    return 0;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "INSERT " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;
    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "DELETE " << table << ' ' << key << endl;
    return 0;
  }

 private:
  std::mutex mutex_;
};

}  // namespace ycsbc

int main(int argc, const char *argv[]) {
  ycsbc::DB *basic_db = new ycsbc::BasicDB();
  ycsbc::RunBench(argc, argv, basic_db);
}
