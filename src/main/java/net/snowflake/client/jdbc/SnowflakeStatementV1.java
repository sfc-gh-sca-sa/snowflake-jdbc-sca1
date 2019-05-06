/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.StmtUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.snowflake.client.jdbc.ErrorCode.FEATURE_UNSUPPORTED;

/**
 * Snowflake statement
 */
class SnowflakeStatementV1 implements Statement, SnowflakeStatement
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeStatementV1.class);

  private static final int NO_UPDATES = -1;

  protected final SnowflakeConnectionV1 connection;

  protected final int resultSetType;
  protected final int resultSetConcurrency;
  protected final int resultSetHoldability;

  /*
   * The maximum number of rows this statement ( should return (0 => all rows).
   */
  private int maxRows = 0;

  // Refer to all open resultSets from this statement
  private final Set<ResultSet> openResultSets = Collections.synchronizedSet(new HashSet<>());

  // result set currently in use
  private ResultSet resultSet = null;

  private int fetchSize = 50;

  private Boolean isClosed = false;

  private int updateCount = NO_UPDATES;

  // timeout in seconds
  private int queryTimeout = 0;

  // max field size limited to 16MB
  private final int maxFieldSize = 16777216;

  SFStatement sfStatement;

  private boolean poolable;

  /**
   * Snowflake query ID from the latest executed query
   */
  private String queryID;

  /**
   * Snowflake query IDs from the latest executed batch
   */
  private List<String> batchQueryIDs = new LinkedList<>();

  /**
   * batch of sql strings added by addBatch
   */
  protected final List<BatchEntry> batch = new ArrayList<>();

  private SQLWarning sqlWarnings;

  /**
   * Construct SnowflakeStatementV1
   *
   * @param connection           connection object
   * @param resultSetType        result set type: ResultSet.TYPE_FORWARD_ONLY.
   * @param resultSetConcurrency result set concurrency: ResultSet.CONCUR_READ_ONLY.
   * @param resultSetHoldability result set holdability: ResultSet.CLOSE_CURSORS_AT_COMMIT
   * @throws SQLException if any SQL error occurs.
   */
  SnowflakeStatementV1(
      SnowflakeConnectionV1 connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException
  {
    logger.debug(
        " public SnowflakeStatement(SnowflakeConnectionV1 conn)");

    this.connection = connection;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY)
    {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet type %d is not supported.", resultSetType),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
    {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet concurrency %d is not supported.", resultSetConcurrency),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
    {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet holdability %d is not supported.", resultSetHoldability),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;

    sfStatement = new SFStatement(connection.getSfSession());
  }

  protected void raiseSQLExceptionIfStatementIsClosed() throws SQLException
  {
    if (isClosed)
    {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }
  }

  /**
   * Execute SQL query
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException
  {
    raiseSQLExceptionIfStatementIsClosed();
    return executeQueryInternal(sql, null);
  }

  /**
   * Execute an update statement
   *
   * @param sql sql statement
   * @return number of rows updated
   * @throws SQLException if @link{#executeUpdateInternal(String, Map)} throws exception
   */
  @Override
  public int executeUpdate(String sql) throws SQLException
  {
    return executeUpdateInternal(sql, null, true);
  }

  int executeUpdateInternal(String sql,
                            Map<String, ParameterBindingDTO> parameterBindings,
                            boolean updateQueryRequired)
  throws SQLException
  {
    raiseSQLExceptionIfStatementIsClosed();

    if (StmtUtil.checkStageManageCommand(sql) != null)
    {
      throw new SnowflakeSQLException(
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
    }

    SFBaseResultSet sfResultSet;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
                                        SFStatement.CallingMethod.EXECUTE_UPDATE);
      sfResultSet.setSession(this.connection.getSfSession());
      updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      queryID = sfResultSet.getQueryId();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
                                      ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
    finally
    {
      if (resultSet != null)
      {
        openResultSets.add(resultSet);
      }
      resultSet = null;
    }

    if (getUpdateCount() == NO_UPDATES && updateQueryRequired)
    {
      throw new SnowflakeSQLException(
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
    }

    return getUpdateCount();
  }

  /**
   * Internal method for executing a query with bindings accepted.
   *
   * @param sql               sql statement
   * @param parameterBindings parameters bindings
   * @return query result set
   * @throws SQLException if @link{SFStatement.execute(String)} throws exception
   */
  ResultSet executeQueryInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings)
  throws SQLException
  {
    SFBaseResultSet sfResultSet;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
                                        SFStatement.CallingMethod.EXECUTE_QUERY);
      sfResultSet.setSession(this.connection.getSfSession());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
                                      ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    if (resultSet != null)
    {
      openResultSets.add(resultSet);
    }
    resultSet = new SnowflakeResultSetV1(sfResultSet, this);

    return getResultSet();
  }

  /**
   * Execute sql
   *
   * @param sql               sql statement
   * @param parameterBindings a map of binds to use for this query
   * @return whether there is result set or not
   * @throws SQLException if @link{#executeQuery(String)} throws exception
   */
  boolean executeInternal(String sql,
                          Map<String, ParameterBindingDTO> parameterBindings)
  throws SQLException
  {
    raiseSQLExceptionIfStatementIsClosed();
    connection.injectedDelay();

    logger.debug("execute: {}", sql);

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20
        && trimmedSql.toLowerCase().startsWith("set-sf-property"))
    {
      // deprecated: sfsql
      executeSetProperty(sql);
      return false;
    }

    SFBaseResultSet sfResultSet;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
                                        SFStatement.CallingMethod.EXECUTE);
      sfResultSet.setSession(this.connection.getSfSession());
      if (resultSet != null)
      {
        openResultSets.add(resultSet);
      }
      resultSet = new SnowflakeResultSetV1(sfResultSet, this);
      queryID = sfResultSet.getQueryId();

      // Legacy behavior treats update counts as result sets for single-
      // statement execute, so we only treat update counts as update counts
      // if JDBC_EXECUTE_RETURN_COUNT_FOR_DML is set, or if a statement
      // is multi-statement
      if (!sfResultSet.getStatementType().isGenerateResultSet() &&
          (connection.getSfSession().isExecuteReturnCountForDML() ||
           sfStatement.hasChildren()))
      {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
        if (resultSet != null)
        {
          openResultSets.add(resultSet);
        }
        resultSet = null;
        return false;
      }

      updateCount = NO_UPDATES;
      return true;
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
                                      ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  /**
   * @return the query ID of the latest executed query
   * @throws SQLException
   */
  public String getQueryID()
  {
    // return the queryID for the query executed last time
    return queryID;
  }

  /**
   * @return the query IDs of the latest executed batch queries
   * @throws SQLException
   */
  public List<String> getBatchQueryIDs()
  {
    return Collections.unmodifiableList(batchQueryIDs);
  }

  /**
   * Execute sql
   *
   * @param sql sql statement
   * @return whether there is result set or not
   * @throws SQLException if @link{#executeQuery(String)} throws exception
   */
  @Override
  public boolean execute(String sql) throws SQLException
  {
    return executeInternal(sql, null);
  }


  @Override
  public boolean execute(String sql, int autoGeneratedKeys)
  throws SQLException
  {
    logger.debug(
        "public int execute(String sql, int autoGeneratedKeys)");

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
    {
      return execute(sql);
    }
    else
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException
  {
    logger.debug(
        "public boolean execute(String sql, int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException
  {
    logger.debug(
        "public boolean execute(String sql, String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Batch Execute. If one of the commands in the batch failed, JDBC will
   * continuing processing and throw BatchUpdateException after all commands
   * are processed.
   *
   * @return an array of update counts
   * @throws SQLException if any error occurs.
   */
  @Override
  public int[] executeBatch() throws SQLException
  {
    logger.debug("public int[] executeBatch()");

    return executeBatchInternal();
  }

  /**
   * This method will iterate through batch and provide sql and bindings to
   * underlying SFStatement to get result set.
   * <p>
   * Note, array binds use a different code path since only one network
   * roundtrip in the array bind execution case.
   *
   * @return the number of updated rows
   * @throws SQLException raises if statement is closed or any db error occurs
   */
  int[] executeBatchInternal() throws SQLException
  {
    raiseSQLExceptionIfStatementIsClosed();

    SQLException exceptionReturned = null;
    int[] updateCounts = new int[batch.size()];
    batchQueryIDs.clear();
    for (int i = 0; i < batch.size(); i++)
    {
      BatchEntry b = batch.get(i);
      try
      {
        int cnt = this.executeUpdateInternal(
            b.getSql(), b.getParameterBindings(), false);
        if (cnt == NO_UPDATES)
        {
          // in executeBatch we set updateCount to SUCCESS_NO_INFO
          // for successful query with no updates
          cnt = SUCCESS_NO_INFO;
        }
        updateCounts[i] = cnt;
        batchQueryIDs.add(queryID);
      }
      catch (SQLException e)
      {
        exceptionReturned = exceptionReturned == null ? e : exceptionReturned;
        updateCounts[i] = EXECUTE_FAILED;
      }
    }

    if (exceptionReturned != null)
    {
      throw new BatchUpdateException(exceptionReturned.getLocalizedMessage(),
                                     exceptionReturned.getSQLState(),
                                     exceptionReturned.getErrorCode(),
                                     updateCounts,
                                     exceptionReturned);
    }

    return updateCounts;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
  throws SQLException
  {
    logger.debug(
        "public int executeUpdate(String sql, int autoGeneratedKeys)");

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
    {
      return executeUpdate(sql);
    }
    else
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes)
  throws SQLException
  {
    logger.debug(
        "public int executeUpdate(String sql, int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames)
  throws SQLException
  {
    logger.debug(
        "public int executeUpdate(String sql, String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Connection getConnection() throws SQLException
  {
    logger.debug("public Connection getConnection()");
    raiseSQLExceptionIfStatementIsClosed();
    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    logger.debug("public int getFetchDirection()");
    raiseSQLExceptionIfStatementIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    logger.debug("public int getFetchSize()");
    raiseSQLExceptionIfStatementIsClosed();
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
  {
    logger.debug("public ResultSet getGeneratedKeys()");
    raiseSQLExceptionIfStatementIsClosed();
    return new SnowflakeResultSetV1.EmptyResultSet();
  }

  @Override
  public int getMaxFieldSize() throws SQLException
  {
    logger.debug("public int getMaxFieldSize()");
    raiseSQLExceptionIfStatementIsClosed();
    return maxFieldSize;
  }

  @Override
  public int getMaxRows() throws SQLException
  {
    logger.debug("public int getMaxRows()");
    raiseSQLExceptionIfStatementIsClosed();
    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException
  {
    logger.debug("public boolean getMoreResults()");

    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException
  {
    logger.debug("public boolean getMoreResults(int current)");
    raiseSQLExceptionIfStatementIsClosed();

    // clean up the current result set, if it exists
    if (resultSet != null &&
        (current == Statement.CLOSE_CURRENT_RESULT ||
         current == Statement.CLOSE_ALL_RESULTS))
    {
      resultSet.close();
    }


    boolean hasResultSet = sfStatement.getMoreResults(current);
    SFBaseResultSet sfResultSet = sfStatement.getResultSet();

    if (hasResultSet) // result set returned
    {
      sfResultSet.setSession(this.connection.getSfSession());
      if (resultSet != null)
      {
        openResultSets.add(resultSet);
      }
      resultSet = new SnowflakeResultSetV1(sfResultSet, this);
      updateCount = NO_UPDATES;
      return true;
    }
    else if (sfResultSet != null) // update count returned
    {
      if (resultSet != null)
      {
        openResultSets.add(resultSet);
      }
      resultSet = null;
      try
      {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      }
      catch (SFException ex)
      {
        throw new SnowflakeSQLException(ex);
      }
      return false;
    }
    else // no more results
    {
      updateCount = NO_UPDATES;
      return false;
    }
  }

  @Override
  public int getQueryTimeout() throws SQLException
  {
    logger.debug("public int getQueryTimeout()");
    raiseSQLExceptionIfStatementIsClosed();
    return this.queryTimeout;
  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    logger.debug("public ResultSet getResultSet()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException
  {
    logger.debug("public int getResultSetConcurrency()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    logger.debug("public int getResultSetHoldability()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetHoldability;
  }

  @Override
  public int getResultSetType() throws SQLException
  {
    logger.debug("public int getResultSetType()");
    raiseSQLExceptionIfStatementIsClosed();
    return this.resultSetType;
  }

  @Override
  public int getUpdateCount() throws SQLException
  {
    logger.debug("public int getUpdateCount()");
    raiseSQLExceptionIfStatementIsClosed();
    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug("public SQLWarning getWarnings()");
    raiseSQLExceptionIfStatementIsClosed();
    return sqlWarnings;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug("public boolean isClosed()");
    return isClosed; // no exception
  }

  @Override
  public boolean isPoolable() throws SQLException
  {
    logger.debug("public boolean isPoolable()");
    raiseSQLExceptionIfStatementIsClosed();
    return poolable;
  }

  @Override
  public void setCursorName(String name) throws SQLException
  {
    logger.debug("public void setCursorName(String name)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException
  {
    logger.debug("public void setEscapeProcessing(boolean enable)");
    // NOTE: We could raise an exception here, because not implemented
    // but it may break the existing applications. For now returning nothnig.
    // we should revisit.
    raiseSQLExceptionIfStatementIsClosed();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    logger.debug("public void setFetchDirection(int direction)");
    raiseSQLExceptionIfStatementIsClosed();
    if (direction != ResultSet.FETCH_FORWARD)
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    logger.debug("public void setFetchSize(int rows), rows={}", rows);
    raiseSQLExceptionIfStatementIsClosed();
    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException
  {
    logger.debug("public void setMaxFieldSize(int max)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setMaxRows(int max) throws SQLException
  {
    logger.debug("public void setMaxRows(int max)");

    raiseSQLExceptionIfStatementIsClosed();

    this.maxRows = max;
    try
    {
      if (this.sfStatement != null)
      {
        this.sfStatement.addProperty("rows_per_resultset", max);
      }
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException
  {
    logger.debug("public void setPoolable(boolean poolable)");
    raiseSQLExceptionIfStatementIsClosed();

    if (poolable)
    {
      throw new SQLFeatureNotSupportedException();
    }
    this.poolable = poolable;
  }

  /**
   * Sets a parameter at the statement level. Used for internal testing.
   *
   * @param name  parameter name.
   * @param value parameter value.
   * @throws Exception if any SQL error occurs.
   */
  void setParameter(String name, Object value) throws Exception
  {
    logger.debug("public void setParameter");

    try
    {
      if (this.sfStatement != null)
      {
        this.sfStatement.addProperty(name, value);
      }
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex);
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException
  {
    logger.debug("public void setQueryTimeout(int seconds)");
    raiseSQLExceptionIfStatementIsClosed();

    this.queryTimeout = seconds;
    try
    {
      if (this.sfStatement != null)
      {
        this.sfStatement.addProperty("query_timeout", seconds);
      }
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    logger.debug("public boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    if (!iface.isInstance(this))
    {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface
              .getName());
    }
    return (T) this;
  }

  @Override
  public void closeOnCompletion() throws SQLException
  {
    logger.debug("public void closeOnCompletion()");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    logger.debug("public boolean isCloseOnCompletion()");
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException
  {
    close(true);
  }

  public void close(boolean removeClosedStatementFromConnection) throws SQLException
  {
    logger.debug("public void close()");

    // No exception is raised even if the statement is closed.
    if (resultSet != null)
    {
      resultSet.close();
      resultSet = null;
    }
    isClosed = true;
    batch.clear();

    // also make sure to close all created resultSets from this statement
    synchronized (openResultSets)
    {
      for (ResultSet rs : openResultSets)
      {
        if (rs != null && !rs.isClosed())
        {
          if (rs.isWrapperFor(SnowflakeResultSetV1.class))
          {
            rs.unwrap(SnowflakeResultSetV1.class).close(false);
          }
          else
          {
            rs.close();
          }
        }
      }
      openResultSets.clear();
    }
    sfStatement.close();
    if (removeClosedStatementFromConnection)
    {
      connection.removeClosedStatement(this);
    }
  }

  @Override
  public void cancel() throws SQLException
  {
    logger.debug("public void cancel()");
    raiseSQLExceptionIfStatementIsClosed();

    try
    {
      sfStatement.cancel();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex, ex.getSqlState(),
                                      ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug("public void clearWarnings()");
    raiseSQLExceptionIfStatementIsClosed();
    sqlWarnings = null;
  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    logger.debug("public void addBatch(String sql)");

    raiseSQLExceptionIfStatementIsClosed();

    batch.add(new BatchEntry(sql, null));
  }

  @Override
  public void clearBatch() throws SQLException
  {
    logger.debug("public void clearBatch()");

    raiseSQLExceptionIfStatementIsClosed();

    batch.clear();
  }

  private void executeSetProperty(final String sql)
  {
    logger.debug("setting property");

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens.length < 2)
    {
      return;
    }

    if ("tracing".equalsIgnoreCase(tokens[1]))
    {
      if (tokens.length >= 3)
      {
        /*connection.tracingLevel = Level.parse(tokens[2].toUpperCase());
        if (connection.tracingLevel != null)
        {
          Logger snowflakeLogger = Logger.getLogger("net.snowflake");
          snowflakeLogger.setLevel(connection.tracingLevel);
        }*/
      }
    }
    else
    {
      this.sfStatement.executeSetProperty(sql);
    }
  }

  public SFStatement getSfStatement()
  {
    return sfStatement;
  }

  public void removeClosedResultSet(ResultSet rs)
  {
    openResultSets.remove(rs);
  }

  final class BatchEntry
  {
    private final String sql;

    private final Map<String, ParameterBindingDTO> parameterBindings;

    BatchEntry(String sql,
               Map<String, ParameterBindingDTO> parameterBindings)
    {
      this.sql = sql;
      this.parameterBindings = parameterBindings;
    }

    public String getSql()
    {
      return sql;
    }

    public Map<String, ParameterBindingDTO> getParameterBindings()
    {
      return parameterBindings;
    }
  }
}
