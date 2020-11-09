/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;

import java.sql.SQLFeatureNotSupportedException;

import static net.snowflake.client.jdbc.SnowflakeSQLLoggedException.sendTelemetryData;

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
