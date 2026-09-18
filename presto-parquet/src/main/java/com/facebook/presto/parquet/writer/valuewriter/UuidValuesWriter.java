/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet.writer.valuewriter;

import com.facebook.airlift.concurrent.NotThreadSafe;
import com.facebook.presto.common.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Long.reverseBytes;

@NotThreadSafe
public class UuidValuesWriter
        extends PrimitiveValueWriter
{
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(2 * SIZE_OF_LONG)
            .order(ByteOrder.BIG_ENDIAN);

    public UuidValuesWriter(Supplier<ValuesWriter> valuesWriterSupplier, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriterSupplier);
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                writeBuffer.clear();
                // A UUID block holds each half of the value byte-reversed: getLong(i, 0) is
                // Long.reverseBytes(UUID.getMostSignificantBits()), not the most significant bits themselves.
                // See the class javadoc on com.facebook.presto.common.type.UuidType, which documents this
                // representation and states that converting to the byte order a storage format requires is the
                // job of the code reading and writing the value.
                //
                // Parquet requires the canonical order (LogicalTypes.md): "UUID annotates a 16-byte
                // FIXED_LEN_BYTE_ARRAY primitive type. The value is encoded using big-endian, so that
                // 00112233-4455-6677-8899-aabbccddeeff is encoded as the bytes 00 11 22 33 44 55 66 77 88 99 aa
                // bb cc dd ee ff". Iceberg requires the same for its manifest bounds ("16-byte big-endian
                // value"), and derives them from the statistics updated below.
                //
                // reverseBytes turns each block half back into the true most/least significant bits, which the
                // big-endian writeBuffer then lays out most significant byte first. Dropping it would emit each
                // half reversed and silently produce files that no other engine decodes correctly.
                writeBuffer.putLong(reverseBytes(block.getLong(i, 0)));
                writeBuffer.putLong(reverseBytes(block.getLong(i, SIZE_OF_LONG)));
                writeBuffer.flip();
                Binary data = Binary.fromReusedByteBuffer(writeBuffer);
                getValueWriter().writeBytes(data);
                getStatistics().updateStats(data);
            }
        }
    }
}
