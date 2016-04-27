package uk.co.samstokes.connect.bottledwater;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import avro.shaded.com.google.common.collect.ImmutableMap;

public class BottledWaterTask extends SourceTask {

  private Client client;
  private Thread clientThread;
  private int offset = 0; // TODO hacky hacky!

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    String conninfo = props.get("postgres.conninfo");
    client = new Client(conninfo);

    client.start();

    clientThread = new Thread() {
      @Override public void run() {
        boolean interrupted = false;

        while (!interrupted) {
          client.poll();
          client.wwait();

          try {
            sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
            interrupted = true;
          }
        }

        client.destroy();
      }
    };
    clientThread.setDaemon(true);
    clientThread.start();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<Client.Record> records = client.getRecords();
    ++offset;

    Map<String, ?> sourcePartition = ImmutableMap.of(
        "db", "TODO"
    );
    Map<String, ?> sourceOffset = ImmutableMap.of(
        "offset", offset
    );
    int partition = 0;

    return records.stream().map(avroRecord -> {
      String topic = avroRecord.schemaName;
      org.apache.kafka.connect.data.Schema keySchema = avroSchemaToConnectSchema(avroRecord.key.getSchema());
      org.apache.kafka.connect.data.Schema rowSchema = avroSchemaToConnectSchema(avroRecord.row.getSchema());
      Struct key = (Struct) avroValueToConnectValue(avroRecord.key, keySchema);
      Struct row = (Struct) avroValueToConnectValue(avroRecord.row, rowSchema);

      SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, topic, partition, keySchema, key, rowSchema, row);

      return record;
    }).collect(Collectors.toList());
  }

  @Override
  public void stop() {
    clientThread.interrupt();
  }

  private static org.apache.kafka.connect.data.Schema avroSchemaToConnectSchema(
      org.apache.avro.Schema schema) {
    switch (schema.getType()) {
    case RECORD:
      SchemaBuilder builder = SchemaBuilder.struct();

      for (org.apache.avro.Schema.Field field : schema.getFields()) {
        String name = field.name();
        org.apache.kafka.connect.data.Schema fieldSchema = avroSchemaToConnectSchema(field.schema());

        builder = builder.field(name, fieldSchema);
      }

      return builder.build();
    case UNION:
      return avroSchemaToConnectSchema(schema.getTypes().stream().filter(sch -> sch.getType() != Type.NULL).findFirst().get());
    case BOOLEAN: return org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
    case STRING: return org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
    case INT: return org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
    case LONG: return org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
    case FLOAT: return org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
    case DOUBLE: return org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
    case ARRAY:
    case BYTES:
    case ENUM:
    case FIXED:
    case MAP:
    case NULL:
    default:
      throw new RuntimeException("oh noes: got a " + schema);
    }
  }

  private static Object avroValueToConnectValue(Object value, org.apache.kafka.connect.data.Schema schema) {
    switch (schema.type()) {
    case STRUCT:
      GenericRecord record = (GenericRecord) value;
      Struct struct = new Struct(schema);

      for (org.apache.kafka.connect.data.Field field : schema.fields()) {
        Object fieldValue = record.get(field.name());
        struct.put(field.name(), avroValueToConnectValue(fieldValue, field.schema()));
      }

      return struct;
    case BOOLEAN: return (boolean) value;
    case FLOAT32: return (float) value;
    case FLOAT64: return (double) value;
    case INT32: return (int) value;
    case INT64: return (long) value;
    case STRING: return ((Utf8) value).toString();
    case ARRAY:
    case BYTES:
    case INT8:
    case INT16:
    case MAP:
    default:
      throw new RuntimeException("oh noes: got a " + schema);
    }
  }
}
