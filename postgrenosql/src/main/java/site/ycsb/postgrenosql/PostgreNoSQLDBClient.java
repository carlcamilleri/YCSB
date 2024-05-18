/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.postgrenosql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.jdbc.AutoSave;
import site.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreNoSQLDBClient.class);

  /** Count the number of times initialized to teardown on the last. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** Cache for already prepared statements. */
  private static ConcurrentMap<Long,ConcurrentMap<StatementType, PreparedStatement>> cachedStatements;
  private static final ConcurrentMap<StatementType.Type,ConcurrentLinkedQueue<PreparedStatement>> cachedStatementQueue = new ConcurrentHashMap<>();;
  private static final AtomicInteger noOfCachedStatements= new AtomicInteger(0);

  /** The driver to get the connection to postgresql. */
  private static Driver postgrenosqlDriver;

  /** The connection to the database. */
  private static PGPoolingDataSource connectionSource = new PGPoolingDataSource();
  private static final HikariConfig hikariConfig = new HikariConfig();
  private static HikariDataSource hikariDataSource;

  private static volatile String strReadStatement = null;


  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "postgrenosql.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "postgrenosql.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "postgrenosql.passwd";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "postgrenosql.autocommit";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (PostgreNoSQLDBClient.class) {
      if (postgrenosqlDriver != null) {
        return;
      }

      Properties props = getProperties();
      String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
      String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
      String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
      boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

      try {
        Properties tmpProps = new Properties();
        tmpProps.setProperty("user", user);
        tmpProps.setProperty("password", passwd);

        cachedStatements = new ConcurrentHashMap<>();
        //cachedStatementQueue = new ConcurrentHashMap<>();

        postgrenosqlDriver = new Driver();
        connectionSource.setDataSourceName("YCSB");
        connectionSource.setServerName(urls);
        connectionSource.setApplicationName("YCSB");
        connectionSource.setDatabaseName("u_cmsdb");
        connectionSource.setUser(user);
        connectionSource.setPassword(passwd);
        connectionSource.setInitialConnections(0);
        connectionSource.setMaxConnections(1);
        connectionSource.setSslMode("require");
        //connectionSource.setSsl(true);
        connectionSource.setAutosave(AutoSave.NEVER);

        hikariConfig.setJdbcUrl( urls );
        hikariConfig.setUsername( user );
        hikariConfig.setPassword( passwd );
        hikariConfig.setMinimumIdle(20);
        hikariConfig.setMaximumPoolSize(100);
        //hikariConfig.setIdleTimeout(10000);
        hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "2500" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        hikariDataSource = new HikariDataSource(hikariConfig);
        hikariDataSource.getConnection().close();



      } catch (Exception e) {
        LOG.error("Error during initialization: " + e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        cachedStatements.clear();
        cachedStatementQueue.clear();

        connectionSource.close();
        hikariDataSource.close();
      } catch (Exception e) {
        System.err.println("Error in cleanup execution. " + e);
      }
      postgrenosqlDriver = null;
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    PreparedStatement readStatement = null;
    StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
    try {
//      ConcurrentMap<StatementType, PreparedStatement> threadCacheStatements = cachedStatements.get(Thread.currentThread().getId());
//      PreparedStatement readStatement =null;
//      if(threadCacheStatements!=null)
//        readStatement = threadCacheStatements.get(type);
//      if (readStatement == null) {
//        readStatement = createAndCacheReadStatement(type);
//      }
//      ConcurrentLinkedQueue<PreparedStatement> threadCacheStatements = cachedStatementQueue.get(StatementType.Type.READ);
//
//      if (threadCacheStatements == null)
//        readStatement = createAndCacheReadStatement(StatementType.Type.READ, type);
//      else {
//        readStatement = threadCacheStatements.poll();
//        if (readStatement == null) {
//          synchronized (PostgreNoSQLDBClient.class) {
//            readStatement = threadCacheStatements.poll();
//            if (readStatement == null)
//              readStatement = createAndCacheReadStatement(StatementType.Type.READ, type);
//          }
//        }
//      }

      readStatement = createAndCacheReadStatement(StatementType.Type.READ, type);


      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();

      if (!resultSet.next()) {
        resultSet.close();
        readStatement.getConnection().close();
        readStatement.close();

        return Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null) {
          do {
            String field = resultSet.getString(2);
            String value = resultSet.getString(3);
            result.put(field, new StringByteIterator(value));
          } while (resultSet.next());
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      resultSet.close();
      readStatement.getConnection().close();
      readStatement.close();
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
//    } finally {
//
////      if (readStatement != null) {
////        ConcurrentLinkedQueue<PreparedStatement> threadCacheStatements = cachedStatementQueue.get(StatementType.Type.READ);
////        threadCacheStatements.add(readStatement);
////
////      }
//    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
      ConcurrentMap<StatementType, PreparedStatement> threadCacheStatements = cachedStatements.get(Thread.currentThread().getId());
      PreparedStatement scanStatement=null;
      if(threadCacheStatements!=null)
        scanStatement = threadCacheStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type);
      }

      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }

          result.add(values);
        }
      }

      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    PreparedStatement updateStatement = null;
    StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
    try{

      ConcurrentLinkedQueue<PreparedStatement> threadCacheStatements = cachedStatementQueue.get(type);
      if(threadCacheStatements==null)
      {
        updateStatement = createAndCacheUpdateStatement(StatementType.Type.UPDATE, type);
      }
      else {
        updateStatement = threadCacheStatements.poll();
        if (updateStatement == null) {
          updateStatement = createAndCacheUpdateStatement(StatementType.Type.UPDATE, type);
        }
      }


      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      updateStatement.setObject(1, object);
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    } finally {
      if(updateStatement!=null) {
        ConcurrentLinkedQueue<PreparedStatement> threadCacheStatements = cachedStatementQueue.get(StatementType.Type.UPDATE);
        threadCacheStatements.add(updateStatement);
      }
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
      ConcurrentMap<StatementType, PreparedStatement> threadCacheStatements = cachedStatements.get(Thread.currentThread().getId());
      PreparedStatement insertStatement=null;
      if(threadCacheStatements!=null)
        insertStatement = threadCacheStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        jsonObject.put(entry.getKey(), entry.getValue().toString());
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      insertStatement.setObject(2, object);
      insertStatement.setString(1, key);

      int result = insertStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      ConcurrentMap<StatementType, PreparedStatement> threadCacheStatements = cachedStatements.get(Thread.currentThread().getId());
      PreparedStatement deleteStatement=null;
      if(threadCacheStatements!=null)
        deleteStatement = threadCacheStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheReadStatement(StatementType.Type.DELETE,type);
      }

      deleteStatement.setString(1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheReadStatementV1(StatementType readType)  {
    Connection connection = null;
    try {
      connection = connectionSource.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    PreparedStatement readStatement = null;
    try {
      readStatement = connection.prepareStatement(createReadStatement(readType));
    } catch (SQLException e) {
      e.printStackTrace();
    }
    cachedStatements.putIfAbsent(Thread.currentThread().getId(),new ConcurrentHashMap<>());
    PreparedStatement statement = cachedStatements.get(Thread.currentThread().getId()).putIfAbsent(readType, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

//
//  private PreparedStatement createAndCacheReadStatementV2(StatementType.Type readType, StatementType stmType) throws SQLException {
//
//    int statementPoolSize = noOfCachedStatements.get() - cachedStatementQueue.getOrDefault(readType, new ConcurrentLinkedQueue<>()).size();
//    if(statementPoolSize<20) {
//      synchronized (PostgreNoSQLDBClient.class) {
//        statementPoolSize = noOfCachedStatements.get() - cachedStatementQueue.getOrDefault(readType, new ConcurrentLinkedQueue<>()).size();
//        if(statementPoolSize<20) {
//          Connection connection = connectionSource.getConnection();
//          PreparedStatement readStatement = connection.prepareStatement(createReadStatement(stmType));
//          cachedStatementQueue.putIfAbsent(readType, new ConcurrentLinkedQueue<>());
//          noOfCachedStatements.incrementAndGet();
//          return readStatement;
//        }
//      }
//    }
//
//  }


  private PreparedStatement createAndCacheReadStatement(StatementType.Type readType, StatementType stmType) throws SQLException {
    Connection connection = hikariDataSource.getConnection();
    return connection.prepareStatement(createReadStatement(stmType));

  }


  private String createReadStatement(StatementType readType){
    if(strReadStatement == null) {
      synchronized (PostgreNoSQLDBClient.class) {
        if (strReadStatement == null) {
          StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

          if (readType.getFields() == null) {
            read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
          } else {
            for (String field : readType.getFields()) {
              read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
            }
          }

          read.append(" FROM " + readType.getTableName());
          read.append(" WHERE ");
          read.append(PRIMARY_KEY);
          read.append(" = ");
          read.append("?");
          strReadStatement = read.toString();
        }
      }
    }
    return strReadStatement;

  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException{
    Connection connection = connectionSource.getConnection();
    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
    cachedStatements.putIfAbsent(Thread.currentThread().getId(),new ConcurrentHashMap<>());
    PreparedStatement statement = cachedStatements.get(Thread.currentThread().getId()).putIfAbsent(scanType, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createScanStatement(StatementType scanType){
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (scanType.getFields() != null){
      for (String field:scanType.getFields()){
        scan.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
    scan.append(PRIMARY_KEY);
    scan.append(" >= ?");
    scan.append(" ORDER BY ");
    scan.append(PRIMARY_KEY);
    scan.append(" LIMIT ?");

    return scan.toString();
  }

  public PreparedStatement createAndCacheUpdateStatementV1(StatementType updateType)
      throws SQLException{
    Connection connection = connectionSource.getConnection();
    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
    cachedStatements.putIfAbsent(Thread.currentThread().getId(),new ConcurrentHashMap<>());
    PreparedStatement statement = cachedStatements.get(Thread.currentThread().getId()).putIfAbsent(updateType, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }


  private PreparedStatement createAndCacheUpdateStatement(StatementType.Type type, StatementType updateType) throws SQLException {
    Connection connection  = connectionSource.getConnection();
    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
    cachedStatementQueue.putIfAbsent(type,new ConcurrentLinkedQueue<>());
    return updateStatement;
  }



  private String createUpdateStatement(StatementType updateType){
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(COLUMN_NAME + " = " + COLUMN_NAME);
    update.append(" || ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException{
    Connection connection = connectionSource.getConnection();
    PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(insertType));
    cachedStatements.putIfAbsent(Thread.currentThread().getId(),new ConcurrentHashMap<>());
    PreparedStatement statement = cachedStatements.get(Thread.currentThread().getId()).putIfAbsent(insertType, insertStatement);
    if (statement == null) {
      return insertStatement;
    }
    return statement;
  }

  private String createInsertStatement(StatementType insertType){
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException{
    Connection connection = connectionSource.getConnection();
    PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(deleteType));
    cachedStatements.putIfAbsent(Thread.currentThread().getId(),new ConcurrentHashMap<>());
    PreparedStatement statement = cachedStatements.get(Thread.currentThread().getId()).putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType){
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
