/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.jdbc;

import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.hsqldb.Server;

/**
 *
 */
public class JDBCTest extends ClusterTestCase
  {
  String inputFile = "src/test/data/small.txt";
  private Server server;

  public JDBCTest()
    {
    super( "jbdc tap test", false );
    }

  @Override
  public void setUp() throws IOException
    {
    super.setUp();

    server = new Server();
    server.setDatabasePath( 0, "build/db/testing" );
    server.setDatabaseName( 0, "testing" );
    server.start();
    }

  @Override
  public void tearDown() throws IOException
    {
    super.tearDown();

    server.stop();
    }

  public void testJDBC() throws IOException
    {

    // CREATE NEW TABLE FROM SOURCE

    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), "\\s" ) );

    String url = "jdbc:hsqldb:hsql://localhost/testing";
    String driver = "org.hsqldb.jdbcDriver";
    String tableName = "testingtable";
    String[] columnNames = {"num", "lower", "upper"};
    String[] columnDefs = {"VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL", "VARCHAR(100) NOT NULL"};
    String[] primaryKeys = {"num", "lower"};
    TableDesc tableDesc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );

    Tap replaceTap = new JDBCTap( url, driver, tableDesc, new JDBCScheme( columnNames ), SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( getProperties() ).connect( source, replaceTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/jdbc", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( getProperties() ).connect( replaceTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 13 );

    // READ DATA FROM TEXT FILE AND UPDATE TABLE

    JDBCScheme jdbcScheme = new JDBCScheme( columnNames, null, new String[]{"num", "lower"} );
    Tap updateTap = new JDBCTap( url, driver, tableDesc, jdbcScheme, SinkMode.APPEND );

    Flow updateFlow = new FlowConnector( getProperties() ).connect( sink, updateTap, parsePipe );

    updateFlow.complete();

    verifySink( updateFlow, 13 );

    // READ DATA FROM TABLE INTO TEXT FILE, USING CUSTOM QUERY

    Tap sourceTap = new JDBCTap( url, driver, new JDBCScheme( columnNames, "select num, lower, upper from testingtable as testingtable", "select count(*) from testingtable" ) );

    Pipe readPipe = new Each( "read", new Identity() );

    Flow readFlow = new FlowConnector( getProperties() ).connect( sourceTap, sink, readPipe );

    readFlow.complete();

    verifySink( readFlow, 13 );
    }

  private void verifySink( Flow flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();

    while( iterator.hasNext() )
      {
      count++;
      System.out.println( "iterator.next() = " + iterator.next() );
      }

    iterator.close();

    assertEquals( "wrong number of values", expects, count );
    }

  }
