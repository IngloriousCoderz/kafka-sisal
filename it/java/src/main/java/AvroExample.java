import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroExample {
  public static void main(String[] args) throws IOException {
    // 1. Schema definition
    String schemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{ \"name\": \"name\", \"type\": \"string\" },"
        + "{ \"name\": \"id\", \"type\": \"int\" }"
        + "]"
        + "}";

    Schema schema = new Schema.Parser().parse(schemaStr);

    GenericRecord user = new GenericData.Record(schema);
    user.put("name", "Matteo Antony");
    user.put("id", 123);

    DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write(user, encoder);
    encoder.flush();
    byte[] avroBytes = outputStream.toByteArray();
    outputStream.close();

    System.out.println("Avro Serialized (bytes): " + avroBytes.length + " bytes");

    DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(avroBytes);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    GenericRecord deserializedUser = datumReader.read(null, decoder);
    inputStream.close();

    System.out.println("Deserialized name: " + deserializedUser.get("name"));
  }
}
