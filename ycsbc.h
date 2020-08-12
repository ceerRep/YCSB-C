#pragma once

#include "core/core_workload.h"
#include "core/db.h"

namespace ycsbc {

void RunBench(int argc, const char* argv[], DB* db);
}
