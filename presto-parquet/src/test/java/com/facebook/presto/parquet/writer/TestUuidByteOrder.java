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
package com.facebook.presto.parquet.writer;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOConverter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.type.UuidType.javaUuidToPrestoUuid;
import static com.facebook.presto.common.type.UuidType.prestoUuidToJavaUuid;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.writer.TestParquetWriter.createParquetWriter;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * The Parquet specification (LogicalTypes.md) requires UUID values to be stored as 16 canonical big-endian
 * bytes, most significant byte first: "The value is encoded using big-endian, so that
 * 00112233-4455-6677-8899-aabbccddeeff is encoded as the bytes 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee
 * ff". Iceberg requires the same for the manifest bounds it derives from the Parquet statistics.
 * <p>
 * A UUID block inside Presto stores each half byte-reversed instead, so the reader and the writer both have
 * to convert - see the class javadoc on {@link UuidType}.
 * <p>
 * Round-trip tests alone cannot protect this, because omitting the conversion on both sides cancels out and
 * still round-trips. The absolute byte order is therefore pinned from two directions that do not depend on
 * the other side of Presto's own code: the bytes the writer produces are compared against the
 * specification's worked example, and files written by DuckDB, which encodes the logical type per the
 * specification, are read back.
 */
@Test(singleThreaded = true)
public class TestUuidByteOrder
{
    /** The worked example given in the Parquet specification for the UUID logical type. */
    private static final UUID SPEC_EXAMPLE = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final byte[] SPEC_EXAMPLE_BYTES = {
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            (byte) 0x88, (byte) 0x99, (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff};

    private static final List<Type> TYPES = ImmutableList.of(UuidType.UUID);
    private static final List<String> NAMES = ImmutableList.of("u");

    private File temporaryDirectory;

    @DataProvider(name = "batchReadEnabled")
    public static Object[][] batchReadEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "writerVersion")
    public static Object[][] writerVersion()
    {
        return new Object[][] {{PARQUET_1_0}, {PARQUET_2_0}};
    }

    @DataProvider(name = "writerVersionAndBatchRead")
    public static Object[][] writerVersionAndBatchRead()
    {
        return new Object[][] {
                {PARQUET_1_0, true},
                {PARQUET_1_0, false},
                {PARQUET_2_0, true},
                {PARQUET_2_0, false}};
    }

    /**
     * The bytes the writer puts on disk must be the canonical big-endian encoding, independently of how
     * Presto's own reader happens to interpret them. Asserted through the Parquet column statistics, which
     * hold the raw min/max bytes and are also what Iceberg turns into manifest lower/upper bounds.
     */
    @Test(dataProvider = "writerVersion")
    public void testWrittenBytesAreCanonicalBigEndian(WriterVersion writerVersion)
            throws Exception
    {
        File parquetFile = writeUuids(ImmutableList.of(SPEC_EXAMPLE), writerVersion);

        FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, parquetFile.length(), Optional.empty(), false).getParquetMetadata();
        byte[] minBytes = metadata.getBlocks().get(0).getColumns().get(0).getStatistics().getMinBytes();
        byte[] maxBytes = metadata.getBlocks().get(0).getColumns().get(0).getStatistics().getMaxBytes();

        assertEquals(minBytes, SPEC_EXAMPLE_BYTES, "min bytes were " + hex(minBytes) + ", expected " + hex(SPEC_EXAMPLE_BYTES));
        assertEquals(maxBytes, SPEC_EXAMPLE_BYTES, "max bytes were " + hex(maxBytes) + ", expected " + hex(SPEC_EXAMPLE_BYTES));
    }

    /**
     * Many distinct values, so the column is not dictionary encoded. Under PARQUET_1_0 this is the plain
     * encoding; under PARQUET_2_0 parquet-mr falls back to DELTA_BYTE_ARRAY once it abandons the dictionary,
     * which is the only way to reach FixedLenByteArrayUuidDeltaValuesDecoder.
     */
    @Test(dataProvider = "writerVersionAndBatchRead")
    public void testRoundTripDistinctValues(WriterVersion writerVersion, boolean batchReadEnabled)
            throws Exception
    {
        List<UUID> values = new ArrayList<>(ImmutableList.of(
                SPEC_EXAMPLE,
                UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e1"),
                UUID.fromString("00000000-0000-0000-0000-000000000000"),
                UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                UUID.fromString("00000000-0000-0000-ffff-ffffffffffff"),
                UUID.fromString("ffffffff-ffff-ffff-0000-000000000000")));
        for (int i = 0; i < 512; i++) {
            values.add(randomUUID());
        }
        values.add(null);

        assertEquals(readUuids(writeUuids(values, writerVersion), values.size(), batchReadEnabled), values);
    }

    /**
     * A handful of values repeated many times. Under PARQUET_2_0 parquet-mr dictionary encodes
     * FIXED_LEN_BYTE_ARRAY, which is the only way to reach UuidRLEDictionaryValuesDecoder from a file Presto
     * wrote itself. The PARQUET_1_0 values writer factory has no dictionary writer for FIXED_LEN_BYTE_ARRAY,
     * so that combination stays on the plain path.
     */
    @Test(dataProvider = "writerVersionAndBatchRead")
    public void testRoundTripRepeatedValues(WriterVersion writerVersion, boolean batchReadEnabled)
            throws Exception
    {
        List<UUID> distinct = ImmutableList.of(
                SPEC_EXAMPLE,
                UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e1"),
                UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        List<UUID> values = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            values.add(distinct.get(i % distinct.size()));
        }
        values.add(null);

        assertEquals(readUuids(writeUuids(values, writerVersion), values.size(), batchReadEnabled), values);
    }

    /**
     * Reads a plain encoded file written by DuckDB. This is the check that Presto agrees with other engines
     * rather than only with itself: it fails if the reader applies no byte-order conversion, even though a
     * Presto-written file would still round-trip in that case.
     * <p>
     * The resource was produced with the DuckDB CLI:
     * <pre>
     * COPY (SELECT * FROM (VALUES
     *     (1, CAST('00112233-4455-6677-8899-aabbccddeeff' AS uuid)),
     *     (2, CAST('d2177dd0-eaa2-11de-a572-001b779c76e1' AS uuid)),
     *     (3, CAST('00000000-0000-0000-0000-000000000000' AS uuid)),
     *     (4, CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS uuid)),
     *     (5, NULL)) t(id, u))
     * TO 'written_by_duckdb.parquet' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED);
     * </pre>
     */
    @Test(dataProvider = "batchReadEnabled")
    public void testReadFileWrittenByDuckDb(boolean batchReadEnabled)
            throws Exception
    {
        List<UUID> expected = Arrays.asList(
                SPEC_EXAMPLE,
                UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e1"),
                UUID.fromString("00000000-0000-0000-0000-000000000000"),
                UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                null);

        // the uuid column is the second field in the DuckDB schema
        assertEquals(readUuids(resource("uuid/written_by_duckdb.parquet"), expected.size(), batchReadEnabled, 1), expected);
    }

    /**
     * Reads a PLAIN_DICTIONARY encoded file written by DuckDB. Presto's own PARQUET_1_0 writer never
     * dictionary encodes FIXED_LEN_BYTE_ARRAY, so without a foreign file this decoder would only be covered
     * by the PARQUET_2_0 round trip, which cannot detect a conversion missing on both sides.
     * <p>
     * The resource was produced with the DuckDB CLI:
     * <pre>
     * COPY (SELECT CASE WHEN i = 599 THEN NULL ELSE CAST([
     *           '00112233-4455-6677-8899-aabbccddeeff',
     *           'd2177dd0-eaa2-11de-a572-001b779c76e1',
     *           'ffffffff-ffff-ffff-ffff-ffffffffffff'][(i%3)+1] AS uuid) END AS u
     *       FROM range(0, 600) t(i))
     * TO 'dictionary_written_by_duckdb.parquet' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED);
     * </pre>
     */
    @Test(dataProvider = "batchReadEnabled")
    public void testReadDictionaryEncodedFileWrittenByDuckDb(boolean batchReadEnabled)
            throws Exception
    {
        List<UUID> distinct = ImmutableList.of(
                SPEC_EXAMPLE,
                UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e1"),
                UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        List<UUID> expected = new ArrayList<>();
        for (int i = 0; i < 599; i++) {
            expected.add(distinct.get(i % distinct.size()));
        }
        expected.add(null);

        assertEquals(readUuids(resource("uuid/dictionary_written_by_duckdb.parquet"), expected.size(), batchReadEnabled), expected);
    }

    private File resource(String name)
            throws Exception
    {
        URL resource = getClass().getClassLoader().getResource(name);
        assertNotNull(resource, name + " is missing");
        return new File(resource.toURI());
    }

    private File writeUuids(List<UUID> values, WriterVersion writerVersion)
            throws Exception
    {
        if (temporaryDirectory == null) {
            temporaryDirectory = createTempDir();
        }
        File parquetFile = new File(temporaryDirectory, randomUUID() + ".parquet");

        ParquetWriterOptions options = ParquetWriterOptions.builder()
                .setWriterVersion(writerVersion)
                .setMaxPageSize(DataSize.succinctBytes(1000))
                .setMaxBlockSize(DataSize.succinctBytes(15000))
                .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                .build();

        try (ParquetWriter writer = createParquetWriter(parquetFile, TYPES, NAMES, options, CompressionCodecName.UNCOMPRESSED)) {
            PageBuilder pageBuilder = new PageBuilder(values.size(), TYPES);
            for (UUID value : values) {
                if (value == null) {
                    pageBuilder.getBlockBuilder(0).appendNull();
                }
                else {
                    UuidType.UUID.writeSlice(pageBuilder.getBlockBuilder(0), javaUuidToPrestoUuid(value));
                }
                pageBuilder.declarePosition();
            }
            writer.write(pageBuilder.build());
        }
        return parquetFile;
    }

    private static List<UUID> readUuids(File parquetFile, int expectedCount, boolean batchReadEnabled)
            throws IOException
    {
        return readUuids(parquetFile, expectedCount, batchReadEnabled, 0);
    }

    private static List<UUID> readUuids(File parquetFile, int expectedCount, boolean batchReadEnabled, int channel)
            throws IOException
    {
        FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
        ParquetMetadata metadata = MetadataReader.readFooter(dataSource, parquetFile.length(), Optional.empty(), false).getParquetMetadata();
        MessageType schema = metadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(schema, schema);

        Field field = ColumnIOConverter.constructField(UuidType.UUID, messageColumnIO.getChild(channel))
                .orElseThrow(() -> new IllegalStateException("could not construct a UUID field"));

        ParquetReader reader = new ParquetReader(
                messageColumnIO,
                metadata.getBlocks(),
                Optional.empty(),
                dataSource,
                newSimpleAggregatedMemoryContext(),
                new DataSize(16, MEGABYTE),
                batchReadEnabled,
                false,
                null,
                null,
                false,
                Optional.empty(),
                Optional.of(DateTimeZone.UTC));

        List<UUID> read = new ArrayList<>();
        while (reader.nextBatch() > 0) {
            Block block = reader.readBlock(field);
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    read.add(null);
                }
                else {
                    Slice slice = UuidType.UUID.getSlice(block, position);
                    read.add(prestoUuidToJavaUuid(slice));
                }
            }
        }
        assertEquals(read.size(), expectedCount, "unexpected row count");
        return Collections.unmodifiableList(read);
    }

    private static String hex(byte[] bytes)
    {
        StringBuilder builder = new StringBuilder();
        for (byte b : requireNonNull(bytes, "bytes is null")) {
            builder.append(String.format("%02x ", b));
        }
        return builder.toString().trim();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (temporaryDirectory != null) {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
            temporaryDirectory = null;
        }
    }
}
