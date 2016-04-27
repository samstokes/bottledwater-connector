package uk.co.samstokes.connect.bottledwater;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import avro.shaded.com.google.common.collect.ImmutableMap;

public class BottledWaterConnector extends SourceConnector {

  String conninfo;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    conninfo = props.get("postgres.conninfo");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return BottledWaterTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return Collections.singletonList(ImmutableMap.of(
          "postgres.conninfo", conninfo
    ));
  }

  @Override
  public void stop() {}
}
