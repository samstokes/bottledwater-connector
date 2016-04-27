package uk.co.samstokes.connect.bottledwater;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.sun.jna.Callback;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class Client {
  static {
    Native.register("bottledwater");
  }

  private static native Pointer new_client(String conninfo);
  private static native int start(Pointer client);
  private static native void destroy(Pointer client);

  private static native Pointer get_client_error(Pointer client);
  private static native int client_poll(Pointer client);
  private static native int client_wait(Pointer client);
  private static native void client_fsync(Pointer client, long fsync_lsn);

  private static native void free(Pointer ptr);

  @FunctionalInterface
  interface insert_row_cb extends Callback {
    int invoke(Pointer context, long wal_pos, long relid,
            Pointer key_bin, int key_len, Pointer key_val,
            Pointer row_bin, int row_len, Pointer row_val);
  }
  private static native void client_on_insert_row(Pointer client, insert_row_cb callback);

  @FunctionalInterface
  interface table_schema_cb extends Callback {
    int invoke(Pointer context, long wal_pos, long relid,
            Pointer key_schema_json, int key_schema_len, Pointer key_schema,
            Pointer row_schema_json, int row_schema_len, Pointer row_schema);
  }
  private static native void client_on_table_schema(Pointer client, table_schema_cb callback);


  private Pointer client;
  private final Map<Long, Schema[]> schemata = new HashMap<>();
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private final Object semaphore = new Object();

  public static class Record {
    public String schemaName;
    GenericRecord key;
    GenericRecord row;
  }

  private final List<Record> records = new ArrayList<>();

  public Client(String conninfo) {
    client = new_client(conninfo);
    if (client == null)
      throw new RuntimeException("failed to allocate client");

    client_on_table_schema(client, on_table_schema);
    client_on_insert_row(client, on_insert_row);
  }

  public void start() {
    int err = start(client);
    if (err != 0)
      throw new RuntimeException("failed to start: " + error());
  }

  public void poll() {
    int err = client_poll(client);
    if (err != 0)
      throw new RuntimeException("failed to poll: " + error());
  }

  public void wwait() {
    int err = client_wait(client);
    if (err != 0)
      throw new RuntimeException("failed to wait: " + error());
  }

  private String error() {
    Pointer errptr = get_client_error(client);
    String err = errptr.getString(0);
    free(errptr);
    return err;
  }

  private void fsync(long fsync_lsn) {
    client_fsync(client, fsync_lsn);
  }

  public List<Record> getRecords() throws InterruptedException {
    synchronized (semaphore) {
      while (records.isEmpty()) {
        semaphore.wait();
      }

      List<Record> retrieved = new ArrayList<>(records);
      records.clear();
      return retrieved;
    }
  }

  public void destroy() {
    if (client != null) {
      destroy(client);
      client = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    destroy(client);
    super.finalize();
  }

  private final table_schema_cb on_table_schema = (Pointer context, long wal_pos, long relid,
        Pointer key_schema_json, int key_schema_len, Pointer _key_schema,
        Pointer row_schema_json, int row_schema_len, Pointer _row_schema) -> {
    try {
      String key_schema_json_j = new String(key_schema_json.getByteArray(0, key_schema_len));
      Schema key_schema = new Schema.Parser().parse(key_schema_json_j);

      String row_schema_json_j = new String(row_schema_json.getByteArray(0, row_schema_len));
      Schema row_schema = new Schema.Parser().parse(row_schema_json_j);

      schemata.put(relid, new Schema[] { key_schema, row_schema });
      return 0;
    } catch (RuntimeException e) {
      e.printStackTrace();
      return 1;
    }
  };
  private final insert_row_cb on_insert_row = (Pointer context, long wal_pos, long relid,
        Pointer key_bin, int key_len, Pointer key_val,
        Pointer row_bin, int row_len, Pointer row_val) -> {
    try {
      Schema[] schemas = schemata.get(relid);
      Schema key_schema = schemas[0];
      Schema row_schema = schemas[1];

      DatumReader<GenericRecord> key_reader = new GenericDatumReader<GenericRecord>(key_schema);
      byte[] key_bytes = key_bin.getByteArray(0, key_len);
      BinaryDecoder key_decoder = decoderFactory.binaryDecoder(key_bytes, null);
      GenericRecord key = key_reader.read(null, key_decoder);

      DatumReader<GenericRecord> row_reader = new GenericDatumReader<GenericRecord>(row_schema);
      byte[] row_bytes = row_bin.getByteArray(0, row_len);
      BinaryDecoder row_decoder = decoderFactory.binaryDecoder(row_bytes, null);
      GenericRecord row = row_reader.read(null, row_decoder);

      synchronized (semaphore) {
        Record record = new Record();
        record.schemaName = row_schema.getName();
        record.key = key;
        record.row = row;
        records.add(record);

        semaphore.notify();
      }

      // TODO obviously the wrong place for this
      fsync(wal_pos);

      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  };


  /* TODO */
  public static void main(String[] args) throws InterruptedException {
    String conninfo = String.format("hostaddr=%s port=%s dbname=postgres user=postgres",
        System.getenv("POSTGRES_PORT_5432_TCP_ADDR"),
        System.getenv("POSTGRES_PORT_5432_TCP_PORT"));

    Client client = new Client(conninfo);

    client.start();

    Thread clientThread = new Thread() {
      @Override public void run() {
        while (true) {
          client.poll();
          client.wwait();

          try {
            sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
    clientThread.setDaemon(true);
    clientThread.start();

    while (true) {
      System.out.println("Checking for records");
      List<Record> records = client.getRecords();
      for (Record record : records) {
        System.out.printf("%s: %s -> %s\n", record.schemaName, record.key, record.row);
      }
    }
  }
}
