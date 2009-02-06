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

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Fields;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

/**
 * Class JDBCScheme ...
 */
public class JDBCScheme extends Scheme
  {
  private String[] columns;
  private String orderBy;

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   * @param orderBy of type String
   */
  public JDBCScheme( String[] columns, String orderBy )
    {
    super( new Fields( columns ), new Fields( columns ) );

    this.columns = columns;
    this.orderBy = orderBy;
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   */
  public JDBCScheme( String[] columns )
    {
    this( columns, null );
    }

  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    String tableName = ( (JDBCTap) tap ).getTableName();
    DBInputFormat.setInput( conf, TupleRecord.class, tableName, null, orderBy, columns );
    }

  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    String tableName = ( (JDBCTap) tap ).getTableName();

    DBOutputFormat.setOutput( conf, tableName, columns );
    }

  public Tuple source( Object key, Object value )
    {
    return ( (TupleRecord) value ).getTuple();
    }

  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    Tuple tuple = tupleEntry.selectTuple( getSinkFields() );

    outputCollector.collect(  new TupleRecord( tuple ), null );
    }
  }
