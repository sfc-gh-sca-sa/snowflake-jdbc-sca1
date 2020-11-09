package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeSQLLoggedException.sendTelemetryData;

import java.sql.SQLFeatureNotSupportedException;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;

public class SnowflakeLoggedFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public SnowflakeLoggedFeatureNotSupportedException(SFSession session) {
    super();
    sendTelemetryData(
        null,
        "API call to unsupported JDBC function",
        SqlState.FEATURE_NOT_SUPPORTED,
        -1,
        null,
        session,
        this);
  }
}
