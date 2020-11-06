package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import org.junit.Test;

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
          + "\",\"reason\":\""
          + reason
          + "\",\"ErrorNumber\":"
          + vendorCode
          + "}";

  /** Test that creating in-band objectNode looks as expected */
  @Test
  public void testCreateIBValue() {
    ObjectNode ibValue =
        SnowflakeSQLLoggedException.createIBValue(queryId, reason, SQLState, vendorCode, errorCode);
    assertEquals(comparison, ibValue.toString());
  }

  /** Test that creating out-of-band JSONObject contains all attributes it needs */
  @Test
  public void testCreateOOBValue() {
    JSONObject oobValue =
        SnowflakeSQLLoggedException.createOOBValue(
            queryId, reason, SQLState, vendorCode, errorCode);
    assertEquals("client_sql_exception", oobValue.get("type").toString());
    assertEquals("JDBC", oobValue.get("DriverType").toString());
    assertEquals(driverVersion, oobValue.get("DriverVersion").toString());
    assertEquals(queryId, oobValue.get("QueryID").toString());
    assertEquals(reason, oobValue.get("reason").toString());
    assertEquals(SQLState, oobValue.get("SQLState").toString());
    assertEquals(vendorCode, oobValue.get("ErrorNumber"));
    assertEquals(null, oobValue.get("ErrorType"));
  }
}
