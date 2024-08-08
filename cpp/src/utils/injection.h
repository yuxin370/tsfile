/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef COMMON_INJECTION_H
#define COMMON_INJECTION_H

#include <iostream>
#include <map>
#include <string>

namespace common {

// define struct
struct InjectPoint {
    int count_down_;  // left count
};

// define DBUG_EXECUTE_IF
#define DBUG_EXECUTE_IF(inject_point_name, code)           \
    do {                                                   \
        common::MutexGuard mg(g_all_inject_points_mutex);  \
        if (g_all_inject_points.find(inject_point_name) != \
            g_all_inject_points.end()) {                   \
            InjectPoint& inject_point =                    \
                g_all_inject_points[inject_point_name];    \
            if (inject_point.count_down_ <= 0) {           \
                code;                                      \
            }                                              \
            inject_point.count_down_--;                    \
        }                                                  \
    } while (0)

// open injection
#define ENABLE_INJECTION(inject_point_name, count)        \
    do {                                                  \
        common::MutexGuard mg(g_all_inject_points_mutex); \
        g_all_inject_points[inject_point_name] = {count}; \
    } while (0)

// close injection
#define DISABLE_INJECTION(inject_point_name)              \
    do {                                                  \
        common::MutexGuard mg(g_all_inject_points_mutex); \
        g_all_inject_points.erase(inject_point_name);     \
    } while (0)

// the map save all inject points
extern Mutex g_all_inject_points_mutex;
extern std::map<std::string, InjectPoint> g_all_inject_points;

}  // end namespace common

#endif  // COMMON_INJECTION_H
