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

#ifndef COMMON_ALLOCATOR_PAGE_ARENA_H
#define COMMON_ALLOCATOR_PAGE_ARENA_H

#include "alloc_base.h"

namespace common {

/*
 * Not Thread Safe
 */
class PageArena {
   public:
    explicit PageArena(BaseAllocator &base_allocator = g_base_allocator)
        : page_size_(0),
          mid_(__FIRST_MOD_ID),
          base_allocator_(base_allocator),
          dummy_head_() {}
    ~PageArena() { destroy(); }
    void init(uint32_t page_size, AllocModID mid) {
        page_size_ = page_size;
        mid_ = mid;
    }

    char *alloc(uint32_t size);
    FORCE_INLINE void destroy() { reset(); }
    void reset();

#ifdef ENABLE_TEST
    int TEST_get_page_count() const {
        int count = 0;
        Page *p = dummy_head_.next_;
        while (p) {
            p = p->next_;
            count++;
        }
        return count;
    }
#endif

   private:
    class Page {
       public:
        Page() : next_(nullptr), page_end_(nullptr), cur_alloc_(nullptr) {}
        explicit Page(Page *next_page)
            : next_(next_page), page_end_(nullptr), cur_alloc_(nullptr) {}
        Page(uint32_t page_size, Page *next_page) {
            next_ = next_page;
            cur_alloc_ =
                (char *)this + sizeof(Page);  // equals to (char*)(this+1)
            page_end_ = cur_alloc_ + page_size;
        }
        INLINE char *alloc(uint32_t size) {
            if (cur_alloc_ + size > page_end_) {
                return nullptr;
            } else {
                char *ret = cur_alloc_;
                cur_alloc_ += size;
                return ret;
                //        char *ret = cur_alloc_;
                //        cur_alloc_ += size;
                //        int address = reinterpret_cast<uintptr_t>(cur_alloc_);
                //        int new_addr = (address + 3) & (~3);
                //        cur_alloc_ = reinterpret_cast<char *>(new_addr);
                //        return ret;
            }
        }

       public:
        Page *next_;
        char *page_end_;
        char *cur_alloc_;  // buf_'s current offset
    };

   private:
    uint32_t page_size_;
    AllocModID mid_;
    BaseAllocator &base_allocator_;
    Page dummy_head_;
};

}  // end namespace common
#endif  // COMMON_ALLOCATOR_PAGE_ARENA_H
