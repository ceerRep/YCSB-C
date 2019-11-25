//
//  mega_db.h
//  YCSB-C
//
//  Created by Boxuan Zhao on 19/11/20.
//  Copyright (c) 2019 Boxuan Zhao <zhaobx@ruc.edu.cn>.
//

#ifndef YCSB_C_MEGA_DB_H_
#define YCSB_C_MEGA_DB_H_

#include "core/db.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>
#include "core/properties.h"
#include "megakv/megakv.h"

namespace ycsbc {

class MegaDB : public DB {
 public:
  MegaDB() {
    std::cout << "====================================== MegaKV Init "
                 "======================================"
              << std::endl;
    megakv = new mega::MegaKV();
    std::cout << "====================================== MegaKV Init Finish"
                 "======================================"
              << std::endl;
  }
  MegaDB(const MegaDB &) = delete;
  MegaDB(MegaDB &&) = delete;
  ~MegaDB() { delete megakv; }

  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance
  /// globally.
  ///
  void Init() {}

  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance
  /// globally.
  ///
  void Close() {}

  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of field/value pairs for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    result.clear();
    result.push_back(std::make_pair(key, megakv->Get(key)));
    return 0;
  }

  ///
  /// Reads a list of records from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The vector of keys to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  int MultiRead(const std::string &table, const std::vector<std::string> &key,
                const std::vector<std::string> *fields,
                std::vector<std::vector<KVPair>> &result) {
    result.clear();
    const std::vector<std::string> &values = megakv->MultiGet(key);
    assert(key.size() == values.size());
    for (int i = 0; i < key.size(); i++) {
      std::vector<KVPair> key_value_vector;
      key_value_vector.push_back(std::make_pair(key.at(i), values.at(i)));
      result.push_back(key_value_vector);
    }
    return 0;
  }
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error.
  ///
  int Scan(const std::string &table, const std::string &key, int record_count,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {}
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified vector are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A vector of field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {}

  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified vector are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A vector of field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    megakv->Put(key, values.at(0).second);
    return 0;
  }

  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  int Delete(const std::string &table, const std::string &key) {}

 private:
  mega::MegaKV *megakv;
};

}  // namespace ycsbc

#endif  // YCSB_C_DB_H_
