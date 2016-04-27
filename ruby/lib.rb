require 'avro'
require 'ffi'

class FFI::Struct
  def props
    Hash[*members.map {|k| [k, self[k]] }.flatten]
  end

  def inspect
    props.inspect
  end
end

module Lib
  extend FFI::Library
  ffi_lib 'bottledwater'

  callback :table_schema, [:pointer, :uint64, :uint64, :pointer, :size_t, :pointer,
                    :pointer, :size_t, :pointer], :int
  callback :insert_row, [:pointer, :uint64, :uint64, :pointer, :size_t, :pointer,
                    :pointer, :size_t, :pointer], :int

  class ReplStream < FFI::Struct
    layout :slot_name, :string,
           :output_plugin, :string,
           :snapshot_name, :string,
           :conn, :pointer,
           :start_lsn, :uint64,
           :recvd_lsn, :uint64,
           :fsync_lsn, :uint64,
           :last_checkpoint, :uint64,
           :frame_reader, :pointer,
           :status, :int, # 1 = message was processed on last poll; 0 = no data available right now; -1 = stream ended
           :error, [:char, 512]
  end

  class Client < FFI::Struct
    layout :conninfo, :string,
           :app_name, :string,
           :sql_conn, :pointer,
           :replication_stream, ReplStream,
           :allow_unkeyed, :bool,
           :taking_snapshot, :bool,
           :status, :int, # 1 = message was processed on last poll; 0 = no data available right now; -1 = stream ended
           :error, [:char, 512]

    def repl
      self[:replication_stream]
    end
  end

  attach_function :new_client, [:string], Client
  attach_function :start, [Client], :int
  attach_function :destroy, [Client], :void
  attach_function :client_poll, [Client], :int
  attach_function :client_wait, [Client], :int
  attach_function :client_on_insert_row, [Client, :insert_row], :void
  attach_function :client_on_table_schema, [Client, :table_schema], :void
end

include Lib

trap(:INT) { $interrupted = true }
trap(:EXIT) { destroy($client) if $client }

$client = Client.new(Lib.new_client "hostaddr=#{ENV['POSTGRES_PORT_5432_TCP_ADDR']} port=#{ENV['POSTGRES_PORT_5432_TCP_PORT']} dbname=postgres user=postgres")

$readers = {}

OnTableSchema = Proc.new do |context, wal_pos, relid, key_schema_json, key_schema_len, key_schema, row_schema_json, row_schema_len, row_schema|
  begin
    key_schema_json_s = key_schema_json.read_string(key_schema_len)
    key_schema = Avro::Schema.parse(key_schema_json_s)
    key_reader = Avro::IO::DatumReader.new(key_schema)

    row_schema_json_s = row_schema_json.read_string(row_schema_len)
    row_schema = Avro::Schema.parse(row_schema_json_s)
    row_reader = Avro::IO::DatumReader.new(row_schema)

    $readers[relid] = {key: key_reader, row: row_reader}

    0
  rescue => e
    puts e
    1
  end
end
Lib.client_on_table_schema($client, OnTableSchema)
OnInsertRow = Proc.new do |context, wal_pos, relid, key_bin, key_len, key_val, row_bin, row_len, row_val|
  begin
    readers = $readers.fetch(relid)

    key_bytes = key_bin.read_string(key_len)
    key_decoder = Avro::IO::BinaryDecoder.new(StringIO.new(key_bytes))
    key = readers.fetch(:key).read(key_decoder)

    row_bytes = row_bin.read_string(row_len)
    row_decoder = Avro::IO::BinaryDecoder.new(StringIO.new(row_bytes))
    row = readers.fetch(:row).read(row_decoder)

    p key, row

    $client.repl[:fsync_lsn] = $client.repl[:recvd_lsn]
    0
  rescue => e
    puts e
    1
  end
end
Lib.client_on_insert_row($client, OnInsertRow)

Lib.start($client)

while !$interrupted do
  puts "client taking snapshot: #{$client[:taking_snapshot]}"
  err = client_poll($client)
  puts "client_poll: #{err}" if err != 0
  client_wait($client)
  puts "client_wait: #{err}" if err != 0
  sleep 0.1
end
