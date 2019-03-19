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
import java.util.UUID;
import java.util.stream.IntStream;

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
            writer.append("<root>");
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    final String valueXml = String.format("\n<value attr=\"%s\"><nested>%1$s</nested></value>", value);
                    writer.append(valueXml);
                    OFFSETS_BY_INDEX.put(index, writer.getOffset());
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
            writer.append("\n</root>");
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
