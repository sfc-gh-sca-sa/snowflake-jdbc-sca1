package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlFeatureNotSupportedTelemetryTest {

  String queryId = "test-query-idfake";
  String reason = "Reason for result failure";
  String SQLState = "00000";
  int vendorCode = 27;
  ErrorCode errorCode = null;
  String driverVersion = SnowflakeDriver.implementVersion;

  String comparison =
      "{\"type\":\"client_sql_exception\",\"DriverType\":\"JDBC\",\"DriverVersion\":\""
          + driverVersion
          + "\","
          + "\"QueryID\":\""
          + queryId
          + "\",\"SQLState\":\""
          + SQLState
          + "\",\"ErrorNumber\":"
          + vendorCode
          + "}";

  /** Test that creating in-band objectNode looks as expected */
  @Test
  public void testCreateIBValue() {
    ObjectNode ibValue =
        SnowflakeSQLLoggedException.createIBValue(reason, SQLState, vendorCode);
    assertEquals(comparison, ibValue.toString());
  }

  /** Test that creating out-of-band JSONObject contains all attributes it needs */
  @Test
  public void testCreateOOBValue() {
    JSONObject oobValue =
        SnowflakeSQLLoggedException.createOOBValue(
            queryId, SQLState, vendorCode);
    assertEquals("client_sql_exception", oobValue.get("type").toString());
    assertEquals("JDBC", oobValue.get("DriverType").toString());
    assertEquals(driverVersion, oobValue.get("DriverVersion").toString());
    assertEquals(queryId, oobValue.get("QueryID").toString());
    assertEquals(SQLState, oobValue.get("SQLState").toString());
    assertEquals(vendorCode, oobValue.get("ErrorNumber"));
  }
}
