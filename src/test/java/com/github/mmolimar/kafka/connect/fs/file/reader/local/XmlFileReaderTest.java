package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.XmlFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XmlFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "xml";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = XmlFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<String, Object>() {{
            put(XmlFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        }};
    }

    private static Path createDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (CountingWriter writer = new CountingWriter(new FileWriter(txtFile))) {
            writer.append("<root>\n");
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    OFFSETS_BY_INDEX.put(index, writer.getOffset());
                    final String valueXml = String.format("<value attr=\"%s\"><nested>%1$s</nested></value>\n", value);
                    writer.append(valueXml);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
            OFFSETS_BY_INDEX.put(NUM_RECORDS, writer.getOffset());
            writer.append("</root>");
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(XmlFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(XmlFileReader.FILE_READER_TEXT_ENCODING, "Cp1252");
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test(expected = UnsupportedCharsetException.class)
    public void invalidFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(XmlFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(XmlFileReader.FILE_READER_TEXT_ENCODING, "invalid_charset");
        }};
        getReader(fs, dataFile, cfg);
    }

    @Override
    protected Offset getOffset(long offset) {
        return new XmlFileReader.TextOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue(record.get(FIELD_NAME_VALUE).toString().
                matches(String.format("<value attr=\"(%s_[a-zA-Z0-9-]+)\"><nested>\\1</nested></value>", index)));
    }

    @Test
    public void seekFile() {
        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue(), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue(), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() , reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS)));
        assertFalse(reader.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void exceededSeek() {
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS)));
        assertFalse(reader.hasNext());
        reader.next();
    }

    @Test
    public void seekLastRecord() {
        int recordIndex = NUM_RECORDS - 1;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() , reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        assertFalse(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex + 1).longValue() , reader.currentOffset().getRecordOffset());
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

    private static class CountingWriter extends Writer {
        private Writer inner;
        private long offset;

        public CountingWriter(Writer writer) {
            this.inner = writer;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            this.inner.write(cbuf, off, len);
            offset += len;
        }

        @Override
        public void flush() throws IOException {
            this.inner.flush();
        }

        @Override
        public void close() throws IOException {
            this.inner.close();
        }

        public long getOffset() {
            return offset;
        }
    }
}
