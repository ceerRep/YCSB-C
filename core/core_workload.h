//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <seastar/core/smp.hh>
#include <string>
#include <vector>
#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"

namespace ycsbc {

enum Operation { INSERT, READ, UPDATE, SCAN, READMODIFYWRITE, MULTIREAD };

class CoreWorkload {
 public:
  ///
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;

  ///
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;

  ///
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;

  ///
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// multi-read transactions.
  ///
  static const std::string MULTIREAD_PROPORTION_PROPERTY;
  static const std::string MULTIREAD_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;

  ///
  /// The name of the property for the scan length distribution.
  /// Options are "uniform", "zipfian" and "static" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;

  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);

  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);

  virtual std::string NextTable() { return table_name_; }
  virtual std::string NextSequenceKey0();     /// Used for loading data
  virtual std::string NextTransactionKey0();  /// Used for transactions
  
  virtual std::string NextSequenceKey(int id);     /// Used for loading data
  virtual std::string NextTransactionKey(int id);  /// Used for transactions

  virtual std::vector<std::string> NextTransactionMultiKey(int len);
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual std::string NextFieldName();
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }

  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }

  CoreWorkload()
      : field_count_(0),
        read_all_fields_(false),
        write_all_fields_(false),
        field_len_generator_(NULL),
        key_generator_(NULL),
        key_chooser_(NULL),
        field_chooser_(NULL),
        scan_len_chooser_(NULL),
        insert_key_sequence_(3),
        ordered_inserts_(true),
        record_count_(0) {}

  virtual ~CoreWorkload() {
    if (field_len_generator_) delete field_len_generator_;
    if (key_generator_) delete key_generator_;
    if (key_chooser_) delete key_chooser_;
    if (field_chooser_) delete field_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }

 protected:
  static Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);

  std::string table_name_;
  int field_count_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  Generator<uint64_t> *key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  bool ordered_inserts_;
  size_t record_count_;

  std::vector<ycsbc::DB::KVPair> pair_values;

  std::vector<std::vector<std::string>> seq_keys;
  std::vector<int> seq_keys_cursor;
  std::vector<std::vector<std::string>> txn_keys;
  std::vector<int> txn_keys_cursor;
};

inline std::string CoreWorkload::NextSequenceKey0() {
  uint64_t key_num = key_generator_->Next();
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextTransactionKey0() {
  uint64_t key_num;
  key_num = key_chooser_->Next();
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextSequenceKey(int id) {
  if (seq_keys_cursor[id] >= seq_keys[id].size()) {
    seq_keys_cursor[id] = 0;
    std::cerr << "not enough seq key" << std::endl;
  }
  return seq_keys[id][seq_keys_cursor[id]++];
}

inline std::string CoreWorkload::NextTransactionKey(int id) {
  if (txn_keys_cursor[id] >= txn_keys[id].size()) {
    txn_keys_cursor[id] = 0;
    std::cerr << "not enough txn key" << std::endl;
  }
  return txn_keys[id][txn_keys_cursor[id]++];
}

inline std::vector<std::string> CoreWorkload::NextTransactionMultiKey(int len) {
  uint64_t key_num;
  std::vector<std::string> keys;
  key_num = key_chooser_->Next();
  for (int i = 0; i < len; i++) keys.push_back(BuildKeyName(key_num + i));
  return keys;
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  return std::string("user").append(std::to_string(key_num));
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string("field").append(std::to_string(field_chooser_->Next()));
}

}  // namespace ycsbc

#endif  // YCSB_C_CORE_WORKLOAD_H_
