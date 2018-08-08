package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.google.common.io.CharStreams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import javax.xml.stream.Location;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class XmlFileReader extends AbstractFileReader<XmlFileReader.XmlRecord> {

    public static final String FIELD_NAME_VALUE_DEFAULT = "value";

    private static final String FILE_READER_TEXT = FILE_READER_PREFIX + "xml.";
    public static final String FILE_READER_TEXT_ENCODING = FILE_READER_TEXT + "encoding";
    private static final String FILE_READER_SEQUENCE_FIELD_NAME_PREFIX = FILE_READER_TEXT + "field_name.";
    public static final String FILE_READER_TEXT_FIELD_NAME_VALUE = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "value";
    private final TextOffset offset;
    private String currentRecord;
    private boolean finished = false;
    private XMLEventReader reader;
    private Schema schema;
    private Charset charset;
    // needed for skipping to a specific record
    private String rootStartTag;
    private boolean closed;

    public XmlFileReader(FileSystem fs, Path filePath) throws IOException {
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

        if (config.get(FILE_READER_TEXT_ENCODING) == null ||
                config.get(FILE_READER_TEXT_ENCODING).toString().equals("")) {
            this.charset = Charset.defaultCharset();
        } else {
            this.charset = Charset.forName(config.get(FILE_READER_TEXT_ENCODING).toString());
        }
        XMLInputFactory f = XMLInputFactory.newInstance();
        try {
            this.reader = f.createXMLEventReader(new InputStreamReader(getFs().open(getFilePath()), this.charset));
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
                        offset.setOffset(event.getLocation().getCharacterOffset());
                        return true;
                    } else if(event.isEndElement()) { // can only happen for root element
                        offset.setOffset(event.getLocation().getCharacterOffset());
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
            XMLInputFactory f = XMLInputFactory.newInstance();
            final InputStreamReader tail = new InputStreamReader(getFs().open(getFilePath()), this.charset);
            CharStreams.skipFully(tail, offset.getRecordOffset());
            Reader reader = CharStreams.join(CharStreams.newReaderSupplier(this.rootStartTag), () -> tail).getInput();
            this.reader = f.createXMLEventReader(reader);
            this.offset.setSeekOffset(offset.getRecordOffset() - this.rootStartTag.length());
            this.currentRecord = null;
            // go to root
            while (this.reader.hasNext() && !this.reader.nextEvent().isStartElement())
                ;
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException("Cannot seek in event reader", e);
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Stream is closed!");
        }
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        try {
            this.closed = true;
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
        checkClosed();
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
