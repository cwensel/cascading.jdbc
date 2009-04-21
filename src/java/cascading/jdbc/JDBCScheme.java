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

import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBOutputFormat;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Class JDBCScheme defines what its parent Tap will select and insert/update into the sql database.
 * <p/>
 * If updateBy column names are given, a SQL UPDATE statement will be generated if the values in those columns
 * for the given Tuple are all not {@code null}. Otherwise an INSERT statement will be generated.
 */
public class JDBCScheme extends Scheme
  {
  private Class<? extends DBInputFormat> inputFormatClass;
  private Class<? extends DBOutputFormat> outputFormatClass;
  private String[] columns;
  private String[] orderBy;
  private String[] updateBy;
  private Fields updateValueFields;
  private Fields updateByFields;
  private Fields columnFields;
  private Tuple updateIfTuple;

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param inputFormatClass  of type Class<? extends DBInputFormat>
   * @param outputFormatClass of type Class<? extends DBOutputFormat>
   * @param columns           of type String[]
   * @param orderBy           of type String[]
   * @param updateBy          of type String[]
   */
  public JDBCScheme( Class<? extends DBInputFormat> inputFormatClass, Class<? extends DBOutputFormat> outputFormatClass, String[] columns, String[] orderBy, String[] updateBy )
    {
    this.columnFields = new Fields( columns );

    setSinkFields( columnFields );
    setSourceFields( columnFields );

    if( updateBy != null && updateBy.length != 0 )
      {
      this.updateBy = updateBy;
      this.updateByFields = new Fields( updateBy );

      if( !this.columnFields.contains( this.updateByFields ) )
        throw new IllegalArgumentException( "columns must contain updateBy column names" );

      this.updateValueFields = columnFields.subtract( updateByFields ).append( updateByFields );
      this.updateIfTuple = Tuple.size( updateByFields.size() ); // all nulls
      }

    this.columns = columns;
    this.orderBy = orderBy;

    this.inputFormatClass = inputFormatClass;
    this.outputFormatClass = outputFormatClass;
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns  of type String[]
   * @param orderBy  of type String[]
   * @param updateBy of type String[]
   */
  public JDBCScheme( String[] columns, String[] orderBy, String[] updateBy )
    {
    this( null, null, columns, orderBy, updateBy );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   * @param orderBy of type String[]
   */
  public JDBCScheme( String[] columns, String[] orderBy )
    {
    this( null, null, columns, orderBy, null );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   */
  public JDBCScheme( String[] columns )
    {
    this( null, null, columns, null, null );
    }

  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    String tableName = ( (JDBCTap) tap ).getTableName();
    String joinedOrderBy = orderBy != null ? Util.join( orderBy, ", " ) : null;
    DBInputFormat.setInput( conf, TupleRecord.class, tableName, null, joinedOrderBy, columns );

    if( inputFormatClass != null )
      conf.setInputFormat( inputFormatClass );
    }

  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    String tableName = ( (JDBCTap) tap ).getTableName();
    DBOutputFormat.setOutput( conf, DBOutputFormat.class, tableName, columns, updateBy );

    if( outputFormatClass != null )
      conf.setOutputFormat( outputFormatClass );
    }

  public Tuple source( Object key, Object value )
    {
    return ( (TupleRecord) value ).getTuple();
    }

  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    if( updateBy != null )
      {
      Tuple allValues = tupleEntry.selectTuple( updateValueFields );
      Tuple updateValues = tupleEntry.selectTuple( updateByFields );

      TupleRecord key = new TupleRecord( allValues );

      if( updateValues.equals( updateIfTuple ) )
        outputCollector.collect( key, null );
      else
        outputCollector.collect( key, key );

      return;
      }

    outputCollector.collect( new TupleRecord( tupleEntry.selectTuple( getSinkFields() ) ), null );
    }
  }
