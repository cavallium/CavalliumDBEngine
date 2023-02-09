/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.cavallium.dbengine.lucene.directory;

import java.io.IOException;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class Lucene90NoCompressionStoredFieldsFormat extends StoredFieldsFormat {

	public static final CompressionMode DUMMY = new CompressionMode() {

		@Override
		public Compressor newCompressor() {
			return DUMMY_COMPRESSOR;
		}

		@Override
		public Decompressor newDecompressor() {
			return DUMMY_DECOMPRESSOR;
		}

		@Override
		public String toString() {
			return "DUMMY";
		}
	};

	private static final Decompressor DUMMY_DECOMPRESSOR = new Decompressor() {

		@Override
		public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes)
				throws IOException {
			assert offset + length <= originalLength;
			if (bytes.bytes.length < originalLength) {
				bytes.bytes = new byte[ArrayUtil.oversize(originalLength, 1)];
			}
			in.readBytes(bytes.bytes, 0, offset + length);
			bytes.offset = offset;
			bytes.length = length;
		}

		@Override
		public Decompressor clone() {
			return this;
		}
	};

	private static final Compressor DUMMY_COMPRESSOR = new Compressor() {

		@Override
		public void compress(ByteBuffersDataInput byteBuffersDataInput, DataOutput dataOutput) {
			dataOutput.copyBytes(byteBuffersDataInput, byteBuffersDataInput.size());
		}

		@Override
		public void close() {
		}
	};

	public Lucene90NoCompressionStoredFieldsFormat() {
	}

	@Override
	public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
			throws IOException {
		return impl().fieldsReader(directory, si, fn, context);
	}

	@Override
	public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
		return impl().fieldsWriter(directory, si, context);
	}

	StoredFieldsFormat impl() {
		return new Lucene90CompressingStoredFieldsFormat("Lucene90StoredFieldsFastData",
				DUMMY,
				BEST_SPEED_BLOCK_LENGTH,
				1024,
				10
		);
	}


	// Shoot for 10 sub blocks of 8kB each.
	private static final int BEST_SPEED_BLOCK_LENGTH = 10 * 8 * 1024;

}
