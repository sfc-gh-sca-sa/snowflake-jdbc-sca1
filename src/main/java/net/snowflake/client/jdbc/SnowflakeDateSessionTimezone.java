package net.snowflake.client.jdbc;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class SnowflakeDateSessionTimezone extends Date {

  TimeZone timezone = TimeZone.getDefault();
  boolean useSessionTimezone = false;

  public SnowflakeDateSessionTimezone(long date, TimeZone timezone, boolean useSessionTimezone) {
    super(date);
    this.timezone = timezone;
    this.useSessionTimezone = useSessionTimezone;
  }

  /**
   * Returns a string representation in UTC so as to display "wallclock time"
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    if (!useSessionTimezone) {
      return super.toString();
    }
    String baseFormat = "yyyy-MM-dd";
    DateFormat formatter = new SimpleDateFormat(baseFormat);
    formatter.setTimeZone(timezone);
    return formatter.format(this);
  }
}
