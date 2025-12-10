import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroExample {
  public static void main(String[] args) throws IOException {
    String schemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"User\","
        + "\"fields\":"
        + "["
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

    System.out.println("Avro Serialized(bytes): " + avroBytes.length + " bytes");
  }
}
