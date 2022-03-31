//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "core_workload.h"
#include "db.h"
#include "utils.h"

#include <seastar/core/future.hh>

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {}

  virtual seastar::future<bool> DoInsert(int id);
  virtual seastar::future<bool> DoTransaction(int id);

  virtual ~Client() {}

 protected:
  virtual int TransactionRead(int id);
  virtual int TransactionReadModifyWrite(int id);
  virtual int TransactionScan(int id);
  virtual int TransactionUpdate(int id);
  virtual int TransactionInsert(int id);
  virtual int TransactionMultiRead(int id);

  DB &db_;
  CoreWorkload &workload_;
};

inline seastar::future<bool> Client::DoInsert(int id) {
  std::string key = workload_.NextSequenceKey(id);
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return seastar::make_ready_future<bool>(db_.Insert(workload_.NextTable(), key, pairs).get() == DB::kOK);
}

inline seastar::future<bool> Client::DoTransaction(int id) {
  int status = -1;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead(id);
      break;
    case UPDATE:
      status = TransactionUpdate(id);
      break;
    case INSERT:
      status = TransactionInsert(id);
      break;
    case SCAN:
      status = TransactionScan(id);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(id);
      break;
    case MULTIREAD:
      status = TransactionMultiRead(id);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return seastar::make_ready_future<bool>(status == DB::kOK);
}

inline int Client::TransactionRead(int id) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey(id);
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result).get();
  } else {
    return db_.Read(table, key, NULL, result).get();
  }
}

inline int Client::TransactionReadModifyWrite(int id) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey(id);
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result).get();
  } else {
    db_.Read(table, key, NULL, result).get();
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values).get();
}

inline int Client::TransactionScan(int id) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey(id);
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result).get();
  } else {
    return db_.Scan(table, key, len, NULL, result).get();
  }
}

inline int Client::TransactionUpdate(int id) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey(id);
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values).get();
}

inline int Client::TransactionInsert(int id) {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey(id);
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values).get();
}

inline int Client::TransactionMultiRead(int id) {
  const std::string &table = workload_.NextTable();
  int len = workload_.NextScanLength();
  const std::vector<std::string> &keys = workload_.NextTransactionMultiKey(len);
  std::vector<std::vector<DB::KVPair>> results;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.MultiRead(table, keys, &fields, results).get();
  } else {
    return db_.MultiRead(table, keys, NULL, results).get();
  }
}

}  // namespace ycsbc

#endif  // YCSB_C_CLIENT_H_
