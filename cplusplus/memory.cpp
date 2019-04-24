/**
 * Copyright 2019 Adobe. All rights reserved.
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

#include "memory.h"
#include <fstream>
#include <string>

using std::string;

const float DEFAULT_PROCESS_MEMORY_THRESHOLD = 3.0f*1024; //in MB

float Memory::memory_threshold_mb = DEFAULT_PROCESS_MEMORY_THRESHOLD;

//Returns: allocated memory, in MB
double Memory::process_memory_usage()
{
    double vm_usage = 0.0;

    string pid, comm, state, ppid, pgrp, session, tty_nr, tpgid, flags;
    string minflt, cminflt, majflt, cmajflt, utime, stime, cutime, cstime;
    string priority, nice, O, itrealvalue, starttime;

    unsigned long long vsize;

    std::ifstream stat_stream("/proc/self/stat");

    if(stat_stream.good())
    {
        stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >>tty_nr
            >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
            >> utime >> stime >> cutime >> cstime >> priority >> nice
            >> O >> itrealvalue >> starttime >> vsize;

        stat_stream.close();

        vm_usage = vsize / (1024.0 * 1024.0); //bytes --> MB
    }

    return vm_usage;
}

float Memory::get_memory_usage_limit_MB()
{
	return Memory::memory_threshold_mb;
}

bool Memory::set_memory_threshold_MB(const float mb)
{
	if (mb > 0.0) {
		Memory::memory_threshold_mb = mb;
		return true;
	}

	return false;
}

//Returns: whether there is enough RAM available for allocating another block
bool Memory::CanAllocateMemory(const long long unsigned memNeeded)
{
	const float memNeededMB = memNeeded / (1024.0 * 1024.0f);

	return process_memory_usage() + memNeededMB < get_memory_usage_limit_MB();
}

