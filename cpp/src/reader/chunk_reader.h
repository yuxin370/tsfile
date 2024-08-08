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

#ifndef READER_CHUNK_READER_H
#define READER_CHUNK_READER_H

#include "common/allocator/my_string.h"
#include "common/tsfile_common.h"
#include "compress/compressor.h"
#include "encoding/decoder.h"
#include "file/read_file.h"
#include "reader/filter/filter.h"

namespace storage {

class ChunkReader {
   public:
    ChunkReader()
        : read_file_(nullptr),
          chunk_meta_(nullptr),
          measurement_name_(),
          chunk_header_(),
          cur_page_header_(),
          in_stream_(),
          file_data_buf_size_(0),
          chunk_visit_offset_(0),
          compressor_(nullptr),
          time_filter_(nullptr),
          time_decoder_(nullptr),
          value_decoder_(nullptr),
          time_in_(),
          value_in_(),
          uncompressed_buf_(nullptr) {}
    int init(ReadFile *read_file, common::String m_name,
             common::TSDataType data_type, Filter *time_filter);
    void reset();
    void destroy();

    bool has_more_data() const {
        return prev_page_not_finish() ||
               (chunk_visit_offset_ - chunk_header_.serialized_size_ <
                chunk_header_.data_size_);
    }
    ChunkHeader &get_chunk_header() { return chunk_header_; }

    /*
     * prepare data buffer, load the chunk_header
     * and the first page_header.
     */
    int load_by_meta(ChunkMeta *meta);

    int get_next_page(common::TsBlock *tsblock, Filter *oneshoot_filter);

   private:
    FORCE_INLINE bool chunk_has_only_one_page() const {
        return chunk_header_.chunk_type_ == ONLY_ONE_PAGE_CHUNK_HEADER_MARKER;
    }
    int alloc_compressor_and_value_decoder(
        common::TSEncoding encoding, common::TSDataType data_type,
        common::CompressionType compression_type);
    int get_cur_page_header();
    int read_from_file_and_rewrap(int want_size = 0);
    bool cur_page_statisify_filter(Filter *filter);
    int skip_cur_page();
    int decode_cur_page_data(common::TsBlock *&ret_tsblock, Filter *filter);
    int decode_tv_buf_into_tsblock(char *time_buf, char *value_buf,
                                   uint32_t time_buf_size,
                                   uint32_t value_buf_size,
                                   common::TsBlock *ret_tsblock,
                                   Filter *filter);
    bool prev_page_not_finish() const {
        return (time_decoder_ && time_decoder_->has_remaining()) ||
               time_in_.has_remaining();
    }

    int decode_tv_buf_into_tsblock_by_datatype(common::ByteStream &time_in,
                                               common::ByteStream &value_in,
                                               common::TsBlock *ret_tsblock,
                                               Filter *filter);
    int i32_DECODE_TYPED_TV_INTO_TSBLOCK(common::ByteStream &time_in,
                                         common::ByteStream &value_in,
                                         common::RowAppender &row_appender,
                                         Filter *filter);

   private:
    ReadFile *read_file_;
    ChunkMeta *chunk_meta_;
    common::String measurement_name_;
    ChunkHeader chunk_header_;
    PageHeader cur_page_header_;

    /*
     * Data reader from file is stored in @in_stream_, and the size
     * is stored in @file_data_buf_size_. Note, in_stream_.total_size_
     * is used to limit deserialization, that is why we still have
     * @file_data_buf_size_.
     *
     * Since we may want keep data of current page (and page header
     * of next page) in memory, we need a byte-size cursor to tell
     * us which byte we are processing, so we have @chunk_visit_offset_
     * it refer to position from the start of chunk_header_,
     * also refer to offset within the chunk (including chunk header).
     * It advanced by step of a page header or a page tv data.
     */
    common::ByteStream in_stream_;
    int32_t file_data_buf_size_;
    uint32_t chunk_visit_offset_;

    // Statistic *page_statistic_;
    Compressor *compressor_;
    Filter *time_filter_;

    Decoder *time_decoder_;
    Decoder *value_decoder_;
    common::ByteStream time_in_;
    common::ByteStream value_in_;
    char *uncompressed_buf_;
};

}  // end namespace storage
#endif  // READER_CHUNK_READER_H
