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

#ifndef ENCODING_DECODER_FACTORY_H
#define ENCODING_DECODER_FACTORY_H

#include "decoder.h"
#include "gorilla_decoder.h"
#include "plain_decoder.h"
#include "ts2diff_decoder.h"

namespace storage {

#define ALLOC_AND_RETURN_DECODER(DecoderType)                                \
    do {                                                                     \
        void *buf =                                                          \
            common::mem_alloc(sizeof(DecoderType), common::MOD_DECODER_OBJ); \
        DecoderType *decoder = nullptr;                                      \
        if (buf != nullptr) {                                                \
            decoder = new (buf) DecoderType;                                 \
        }                                                                    \
        return decoder;                                                      \
    } while (false);

class DecoderFactory {
   public:
    static Decoder *alloc_time_decoder() {
        if (common::g_config_value_.time_encoding_type_ == common::PLAIN) {
            ALLOC_AND_RETURN_DECODER(PlainDecoder);
        } else if (common::g_config_value_.time_encoding_type_ ==
                   common::TS_2DIFF) {
            ALLOC_AND_RETURN_DECODER(LongTS2DIFFDecoder);
        } else {
            // not support now
            ASSERT(false);
            return nullptr;
        }
    }

    static Decoder *alloc_value_decoder(common::TSEncoding encoding,
                                        common::TSDataType data_type) {
        if (encoding == common::PLAIN) {
            ALLOC_AND_RETURN_DECODER(PlainDecoder);
        } else if (encoding == common::GORILLA) {
            if (data_type == common::INT32) {
                ALLOC_AND_RETURN_DECODER(IntGorillaDecoder);
            } else if (data_type == common::INT64) {
                ALLOC_AND_RETURN_DECODER(LongGorillaDecoder);
            } else if (data_type == common::FLOAT) {
                ALLOC_AND_RETURN_DECODER(FloatGorillaDecoder);
            } else if (data_type == common::DOUBLE) {
                ALLOC_AND_RETURN_DECODER(DoubleGorillaDecoder);
            } else {
                ASSERT(false);
                return nullptr;
            }
        } else if (encoding == common::TS_2DIFF) {
            if (data_type == common::INT32) {
                ALLOC_AND_RETURN_DECODER(IntTS2DIFFDecoder);
            } else if (data_type == common::INT64) {
                ALLOC_AND_RETURN_DECODER(LongTS2DIFFDecoder);
            } else if (data_type == common::FLOAT) {
                ALLOC_AND_RETURN_DECODER(FloatTS2DIFFDecoder);
            } else if (data_type == common::DOUBLE) {
                ALLOC_AND_RETURN_DECODER(DoubleTS2DIFFDecoder);
            } else {
                ASSERT(false);
            }
        } else {
            // not support now
            ASSERT(false);
            return nullptr;
        }
        return nullptr;
    }

    static void free(Decoder *decoder) { common::mem_free(decoder); }
};

}  // end namespace storage
#endif  // ENCODING_DECODER_FACTORY_H
