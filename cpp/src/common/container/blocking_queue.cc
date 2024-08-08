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

#include "blocking_queue.h"

namespace common {

BlockingQueue::BlockingQueue() : queue_(), mutex_(), cond_() {
    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_, NULL);
}

BlockingQueue::~BlockingQueue() {
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
}

void BlockingQueue::push(void *data) {
    pthread_mutex_lock(&mutex_);
    queue_.push(data);
    pthread_mutex_unlock(&mutex_);
    /*
     * it is safe to signal after unlock.
     * since pthread_cond_wait is guarantee to unlock and sleep atomically.
     */
    pthread_cond_signal(&cond_);
}

void *BlockingQueue::pop() {
    void *ret_data = NULL;
    pthread_mutex_lock(&mutex_);
    while (queue_.empty()) {
        pthread_cond_wait(&cond_, &mutex_);
    }
    ret_data = queue_.front();
    queue_.pop();
    pthread_mutex_unlock(&mutex_);
    return ret_data;
}

}  // end namespace common