package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class XmlFileReader extends AbstractFileReader<XmlFileReader.XmlRecord> {
    private final static Logger log = LoggerFactory.getLogger(XmlFileReader.class);

    public static final String FIELD_NAME_VALUE_DEFAULT = "value";
    public static final String COMPRESSION_DEFAULT = Compression.AUTO.toString();

    private static final String FILE_READER_TEXT = FILE_READER_PREFIX + "xml.";
    public static final String FILE_READER_TEXT_ENCODING = FILE_READER_TEXT + "encoding";
    public static final String FILE_READER_TEXT_COMPRESSION = FILE_READER_TEXT + "compression";
    private static final String FILE_READER_SEQUENCE_FIELD_NAME_PREFIX = FILE_READER_TEXT + "field_name.";
    public static final String FILE_READER_TEXT_FIELD_NAME_VALUE = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "value";
    private final TextOffset offset;
    private String currentRecord;
    private boolean finished = false;
    private XMLEventReader reader;
    private Schema schema;
    private Charset charset;
    private Compression compression;
    // needed for skipping to a specific record
    private String rootStartTag;
    private final static XMLInputFactory INPUT_FACTORY = XMLInputFactory.newInstance();

    public enum Compression {
        AUTO {
            @Override
            public InputStream open(FileSystem fs, Path filePath) throws IOException {
                final String name = filePath.getName();
                switch(name.substring(name.lastIndexOf('.') + 1).toLowerCase()) {
                    case "zip": return ZIP.open(fs, filePath);
                    case "gz": return GZIP.open(fs, filePath);
                    default: return NONE.open(fs, filePath);
                }
            }
        },
        NONE {
            @Override
            public InputStream open(FileSystem fs, Path filePath) throws IOException {
                return fs.open(filePath);
            }
        },
        ZIP {
            @Override
            public InputStream open(FileSystem fs, Path filePath) throws IOException {
                final ZipInputStream inputStream = new ZipInputStream(NONE.open(fs, filePath));
                ZipEntry entry;
                while((entry = inputStream.getNextEntry()) != null) {
                    if(entry.getName().endsWith(".xml")) {
                        break;
                    }
                }
                return inputStream;
            }
        },
        GZIP {
            @Override
            public InputStream open(FileSystem fs, Path filePath) throws IOException {
                return new GZIPInputStream(NONE.open(fs, filePath), 1_000_000);
            }
        };

        public abstract InputStream open(FileSystem fs, Path filePath) throws IOException;
    }

    static {
        INPUT_FACTORY.setProperty(XMLInputFactory.IS_VALIDATING, false);
        INPUT_FACTORY.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        INPUT_FACTORY.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    }

    public XmlFileReader(FileSystem fs, Path filePath) {
        super(fs, filePath, new XmlToStruct());
        this.offset = new TextOffset(0);
    }

    @Override
    public void configure(Map<String, Object> config) throws IOException {
        String valueFieldName;
        if (config.get(FILE_READER_TEXT_FIELD_NAME_VALUE) == null ||
                config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString().equals("")) {
            valueFieldName = FIELD_NAME_VALUE_DEFAULT;
        } else {
            valueFieldName = config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString();
        }
        this.schema = SchemaBuilder.struct()
                .field(valueFieldName, Schema.STRING_SCHEMA)
                .build();
        this.compression = Compression.valueOf(config.getOrDefault(FILE_READER_TEXT_COMPRESSION, COMPRESSION_DEFAULT).toString());

        if (config.get(FILE_READER_TEXT_ENCODING) == null ||
                config.get(FILE_READER_TEXT_ENCODING).toString().equals("")) {
            this.charset = Charset.defaultCharset();
        } else {
            this.charset = Charset.forName(config.get(FILE_READER_TEXT_ENCODING).toString());
        }
        try {
            this.reader = INPUT_FACTORY.createXMLEventReader(new InputStreamReader(this.compression.open(getFs(), getFilePath()), this.charset));
            // go to root
            XMLEvent event = null;
            while (reader.hasNext() && !(event = this.reader.nextEvent()).isStartElement())
                ;
            if(event != null) {
                final StringWriter writer = new StringWriter();
                event.writeAsEncodedUnicode(writer);
                this.rootStartTag = writer.toString();
            }
        } catch (XMLStreamException e) {
            throw new IOException("Cannot open event reader", e);
        }
    }

    @Override
    public boolean hasNext() {
        checkClosed();
        if (currentRecord != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            while (reader.hasNext()) {
                try {
                    XMLEvent event = reader.nextEvent();
                    if (event.isStartElement()) {
                        currentRecord = readCompleteElement(event);
                        offset.setOffset(reader.peek().getLocation().getCharacterOffset());
                        return true;
                    }
                } catch (XMLStreamException e) {
                    final NoSuchElementException nse = new NoSuchElementException("Cannot parse xml");
                    nse.initCause(e);
                    throw nse;
                }
            }
            finished = true;
            return false;
        }
    }

    private String readCompleteElement(XMLEvent startEvent) throws XMLStreamException {
        int depth = 1;
        final StringWriter writer = new StringWriter();
        startEvent.writeAsEncodedUnicode(writer);
        while(reader.hasNext() && depth > 0) {
            XMLEvent event = reader.nextEvent();
            event.writeAsEncodedUnicode(writer);
            if(event.isStartElement()) depth++;
            else if(event.isEndElement()) depth--;
        }
        return writer.toString();
    }

    @Override
    public void seek(Offset offset) {
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        checkClosed();
        try {
            this.currentRecord = null;
            this.reader.close();
            final PushbackReader tail = new PushbackReader(new InputStreamReader(
                    this.compression.open(getFs(), getFilePath()), this.charset), this.rootStartTag.length());

            if(offset.getRecordOffset() > 0) {
                skipFully(offset, tail);
                tail.unread(this.rootStartTag.toCharArray());
                this.reader = INPUT_FACTORY.createXMLEventReader(tail);
                this.offset.setSeekOffset(offset.getRecordOffset() - this.rootStartTag.length());
            } else {
                this.reader = INPUT_FACTORY.createXMLEventReader(tail);
                this.offset.setSeekOffset(0);
            }
            // go to root
            while (this.reader.hasNext() && !this.reader.nextEvent().isStartElement())
                ;
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException("Cannot seek in event reader", e);
        }
    }
    
    private void skipFully(final Offset offset, final PushbackReader tail) throws IOException {
        long charactersToSkip = offset.getRecordOffset();
        while (charactersToSkip > 0) {
           long skipped = tail.skip(charactersToSkip);
           if (skipped == 0) {
               throw new EOFException();
           }
           charactersToSkip -= skipped;
        }
    }
    
    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        try {
            final XMLEventReader reader = this.reader;
            this.reader = null;
            super.close();
            reader.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected XmlRecord nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        String aux = currentRecord;
        currentRecord = null;

        return new XmlRecord(schema, aux);
    }

    public static class TextOffset implements Offset {
        private long offset, seekOffset;

        public TextOffset(long offset) {
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public long getRecordOffset() {
            return offset + seekOffset;
        }

        public void setSeekOffset(long seekOffset) {
            this.seekOffset = seekOffset;
        }
    }

    static class XmlToStruct implements ReaderAdapter<XmlRecord> {

        @Override
        public Struct apply(XmlRecord record) {
            return new Struct(record.schema)
                    .put(record.schema.fields().get(0), record.value);
        }
    }

    static class XmlRecord {
        private final Schema schema;
        private final String value;

        public XmlRecord(Schema schema, String value) {
            this.schema = schema;
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
