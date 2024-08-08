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

#ifndef READER_SCAN_ITERATOR_H
#define READER_SCAN_ITERATOR_H

#include <stdint.h>

#include "common/container/list.h"
#include "common/seq_tvlist.h"
#include "common/tsblock/tsblock.h"
#include "file/open_file.h"
#include "file/tsfile_io_reader.h"
#include "utils/db_utils.h"
#include "utils/storage_utils.h"

namespace storage {

enum DataRunType {
    DRT_TVLIST = 0,
    DRT_TSFILE = 1,
    DRT_INVALID = 2,
};

/*
 * All KV range are sorted within one DataRun.
 * KV ranges in different runs may overlap with each other.
 *
 * One or more SeqTVLists are a DataRun,
 * One or more TsFileReader are a DataRun.
 */
class DataRun {
   public:
    DataRun(DataRunType run_type, common::ColumnDesc *col_desc,
            common::PageArena *pa)
        : run_type_(run_type),
          col_desc_(col_desc),
          time_range_(),
          tvlist_list_(pa),
          tvlist_list_iter_(),
          tsfile_list_(pa),
          tsfile_list_iter_(),
          io_reader_(),
          ssi_(nullptr),
          tuple_desc_() {}

    int get_next(common::TsBlock *ret_block, TimeRange &ret_time_range,
                 bool alloc);

    const char *get_data_run_type_name(DataRunType type) {
        ASSERT(type == DRT_TVLIST || type == DRT_TSFILE);
        static const char *names[2] = {"TVLIST", "TSFILE"};
        return names[type];
    }
    int remove_tsfile(const common::FileID &file_id);

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &out, DataRun &data_run) {
        out << "type=" << data_run.get_data_run_type_name(data_run.run_type_)
            << ", time_range={start=" << data_run.time_range_.start_time_
            << ", end=" << data_run.time_range_.end_time_ << "}";
        if (DRT_TVLIST == data_run.run_type_) {
            common::SimpleList<SeqTVListBase *>::Iterator it;
            int count = 0;
            out << ", tvlist_list={";
            for (it = data_run.tvlist_list_.begin();
                 it != data_run.tvlist_list_.end(); it++) {
                if (it == data_run.tvlist_list_.begin()) {
                    out << "[" << count << "]" << (void *)it.get();
                } else {
                    out << ", [" << count << "]" << (void *)it.get();
                }
                count++;
            }
            out << "}";
        } else if (DRT_TSFILE == data_run.run_type_) {
            common::SimpleList<OpenFile *>::Iterator it;
            int count = 0;
            out << ", tsfile_list={";
            for (it = data_run.tsfile_list_.begin();
                 it != data_run.tsfile_list_.end(); it++) {
                if (it == data_run.tsfile_list_.begin()) {
                    out << "[" << count << "]" << *it.get();
                } else {
                    out << ", [" << count << "]" << *it.get();
                }
                count++;
            }
            out << "}";
        }
        return out;
    }
#endif

   private:
    int tvlist_get_next(common::TsBlock *ret_block, TimeRange &ret_time_range,
                        bool alloc);
    int fill_tsblock_from_tvlist(SeqTVListBase *tvlist,
                                 common::TsBlock *ret_block,
                                 TimeRange &ret_time_range);
    template <typename T>
    int fill_tsblock_from_typed_tvlist(SeqTVListBase *tvlist,
                                       common::TsBlock *ret_block,
                                       TimeRange &ret_time_range);
    int tsfile_get_next(common::TsBlock *ret_block, TimeRange &ret_time_range,
                        bool alloc);
    int reinit_io_reader(common::SimpleList<OpenFile *>::Iterator &it);
    common::TsBlock *alloc_tsblock();

   public:
    DataRunType run_type_;
    common::ColumnDesc *col_desc_;
    TimeRange time_range_;

    // invalid if run_type_ is DRT_TSFILE
    common::SimpleList<SeqTVListBase *> tvlist_list_;
    common::SimpleList<SeqTVListBase *>::Iterator tvlist_list_iter_;

    // invalid if run_type_ is DRT_TVLIST
    common::SimpleList<OpenFile *> tsfile_list_;
    common::SimpleList<OpenFile *>::Iterator tsfile_list_iter_;
    // TODO may bind TsFileIOReader on OpenFile ?
    TsFileIOReader io_reader_;
    TsFileSeriesScanIterator *ssi_;
    common::TupleDesc tuple_desc_;
};

class DataScanIterator {
   public:
    DataScanIterator()
        : col_desc_(), page_arena_(), data_run_list_(&page_arena_), cursor_() {}
    ~DataScanIterator() {}
    int init() { return common::E_OK; }
    void destory() {
        close();
        page_arena_.destroy();
    }
    void close() {
        // TODO
    }

    DataRun *alloc_data_run(DataRunType run_type);
    FORCE_INLINE int add_data_run(DataRun *data_run) {
        ASSERT(data_run != nullptr);
        return data_run_list_.push_back(data_run);
    }
    void reset_for_retry() { page_arena_.reset(); }

    /*
     * get next tsblock
     * return value
     *        E_OK  -  succ
     *        E_INVALID_ARG - ret_block not init
     *        E_NO_MORE_DATA - reader over
     */
    int get_next(common::TsBlock *block, bool alloc_tsblock = false);
    void set_col_desc(const common::ColumnDesc &col_desc) {
        col_desc_ = col_desc;
    }

#ifndef NDEBUG
    void DEBUG_dump_data_run_list();
#endif

   private:
    common::ColumnDesc col_desc_;
    common::PageArena page_arena_;
    common::SimpleList<DataRun *> data_run_list_;
    common::SimpleList<DataRun *>::Iterator cursor_;
};

}  // end namespace storage
#endif  // READER_SCAN_ITERATOR_H
