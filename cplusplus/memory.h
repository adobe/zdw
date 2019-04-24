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

#ifndef MEMORY_H
#define MEMORY_H

class Memory
{
	Memory(); //unimplemented

	static float memory_threshold_mb;

public:
	static bool CanAllocateMemory(const long long unsigned memNeeded);

	static float get_memory_usage_limit_MB();
	static double process_memory_usage();
	static bool set_memory_threshold_MB(const float mb);
};

#endif
