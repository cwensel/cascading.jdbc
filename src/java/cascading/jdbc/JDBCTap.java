/*
 * Copyright (c) 2009, Your Corporation. All Rights Reserved.
 */

package cascading.jdbc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.TapIterator;
import cascading.tap.hadoop.TapCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.scheme.Scheme;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class JDBCTap is a {@link Tap} subclass that provides read and write access to a RDBMS.
 */
public class JDBCTap extends Tap
  {
  /** Field LOG  */
  private static final Logger LOG = LoggerFactory.getLogger( JDBCTap.class );

  /** Field connectionUrl  */
  String connectionUrl;
  /** Field driverClassName  */
  String driverClassName;
  /** Field tableDesc  */
  TableDesc tableDesc;

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   *
   * @param connectionUrl of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   * @param sinkMode of type SinkMode
   */
  public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.connectionUrl = connectionUrl;
    this.driverClassName = driverClassName;
    this.tableDesc = tableDesc;
    }

  /**
   * Constructor JDBCTap creates a new JDBCTap instance.
   *
   * @param connectionUrl of type String
   * @param driverClassName of type String
   * @param tableDesc of type TableDesc
   * @param scheme of type JDBCScheme
   */
  public JDBCTap( String connectionUrl, String driverClassName, TableDesc tableDesc, JDBCScheme scheme )
    {
    this( connectionUrl, driverClassName, tableDesc, scheme, SinkMode.APPEND );
    }

  /**
   * Method getTableName returns the tableName of this JDBCTap object.
   *
   * @return the tableName (type String) of this JDBCTap object.
   */
  public String getTableName()
    {
    return tableDesc.tableName;
    }

  /**
   * Method getPath returns the path of this JDBCTap object.
   *
   * @return the path (type Path) of this JDBCTap object.
   */
  public Path getPath()
    {
    return new Path( "jdbc:/" + connectionUrl.replaceAll( ":", "_" ) );
    }

  public TupleEntryIterator openForRead( JobConf conf ) throws IOException
    {
    return new TupleEntryIterator( getSourceFields(), new TapIterator( this, conf ) );
    }

  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return new TapCollector( this, conf );
    }

  @Override
  public void sourceInit( JobConf conf ) throws IOException
    {
    // a hack for MultiInputFormat to see that there is a child format
    FileInputFormat.setInputPaths( conf, getPath() );

    DBConfiguration.configureDB( conf, driverClassName, connectionUrl );
    super.sourceInit( conf );
    }

  @Override
  public void sinkInit( JobConf conf ) throws IOException
    {
    // do not delete if initialized from within a task
    if( isReplace() && conf.get( "mapred.task.partition" ) == null )
      deletePath( conf );

    makeDirs( conf );

    DBConfiguration.configureDB( conf, driverClassName, connectionUrl );
    super.sinkInit( conf );
    }

  private Connection createConnection()
    {
    try
      {
      LOG.info( "creating connection: {}", connectionUrl );

      Class.forName( driverClassName );
      Connection connection = DriverManager.getConnection( connectionUrl );
      connection.setAutoCommit( false );

      return connection;
      }
    catch( ClassNotFoundException exception )
      {
      throw new TapException( "unable to load driver class: " + driverClassName, exception );
      }
    catch( SQLException exception )
      {
      throw new TapException( "unable to open connection: " + connectionUrl, exception );
      }
    }

  private void executeUpdate( String updateString )
    {
    Connection connection = null;

    try
      {
      connection = createConnection();

      try
        {
        LOG.info( "executing update: {}", updateString );

        Statement statement = connection.createStatement();
        statement.executeUpdate( updateString );
        connection.commit();
        statement.close();
        }
      catch( SQLException exception )
        {
        throw new TapException( "unable to execute update statement: " + updateString, exception );
        }
      }
    finally
      {
      try
        {
        if( connection != null )
          connection.close();
        }
      catch( SQLException exception )
        {
        // ignore
        LOG.warn( "ignoring connection close exception", exception );
        }
      }
    }

  private void executeQuery( String queryString )
    {
    Connection connection = null;

    try
      {
      connection = createConnection();

      try
        {
        LOG.info( "executing query: {}", queryString );

        Statement statement = connection.createStatement();
        statement.executeQuery( queryString ); // we don't care about results
        connection.commit();
        statement.close();
        }
      catch( SQLException exception )
        {
        throw new TapException( "unable to execute query statement: " + queryString, exception );
        }
      }
    finally
      {
      try
        {
        if( connection != null )
          connection.close();
        }
      catch( SQLException exception )
        {
        // ignore
        LOG.warn( "ignoring connection close exception", exception );
        }
      }
    }

  public boolean makeDirs( JobConf conf ) throws IOException
    {
    if( pathExists( conf ) )
      return true;

    try
      {
      LOG.info( "creating table: {}", tableDesc.tableName );

      executeUpdate( tableDesc.getTableCreateStatement() );
      }
    catch( TapException exception )
      {
      LOG.warn( "unable to create table: {}", tableDesc.tableName, exception );

      return false;
      }

    return true;
    }

  public boolean deletePath( JobConf conf ) throws IOException
    {
    try
      {
      LOG.info( "deleting table: {}", tableDesc.tableName );

      executeUpdate( tableDesc.getTableDropStatement() );
      }
    catch( TapException exception )
      {
      LOG.warn( "unable to drop table: {}", tableDesc.tableName, exception );

      return false;
      }

    return true;
    }

  public boolean pathExists( JobConf conf ) throws IOException
    {
    try
      {
      LOG.info( "test table exists: {}", tableDesc.tableName );

      executeQuery( tableDesc.getTableExistsQuery() );
      }
    catch( TapException exception )
      {
      return false;
      }

    return true;
    }

  public long getPathModified( JobConf conf ) throws IOException
    {
    return System.currentTimeMillis();
    }

  @Override
  public String toString()
    {
    return "JDBCTap{" + "connectionUrl='" + connectionUrl + '\'' + ", driverClassName='" + driverClassName + '\'' + ", tableDesc=" + tableDesc + '}';
    }
  }
