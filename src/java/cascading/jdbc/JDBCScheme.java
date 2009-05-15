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
import java.util.Arrays;

import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBOutputFormat;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
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
 * <p/>
 * Override this class, {@link DBInputFormat}, and {@link DBOutputFormat} to specialize for a given vendor database.
 */
public class JDBCScheme extends Scheme
  {
  private Class<? extends DBInputFormat> inputFormatClass;
  private Class<? extends DBOutputFormat> outputFormatClass;
  private String[] columns;
  private String[] orderBy;
  private String conditions;
  private String[] updateBy;
  private Fields updateValueFields;
  private Fields updateByFields;
  private Fields columnFields;
  private Tuple updateIfTuple;
  private String selectQuery;
  private String countQuery;
  private long limit = -1;

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param inputFormatClass  of type Class<? extends DBInputFormat>
   * @param outputFormatClass of type Class<? extends DBOutputFormat>
   * @param columns           of type String[]
   * @param orderBy           of type String[]
   * @param conditions        of type String
   * @param updateBy          of type String[]
   */
  public JDBCScheme( Class<? extends DBInputFormat> inputFormatClass, Class<? extends DBOutputFormat> outputFormatClass, String[] columns, String[] orderBy, String conditions, long limit, String[] updateBy )
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
    this.conditions = conditions;
    this.limit = limit;

    this.inputFormatClass = inputFormatClass;
    this.outputFormatClass = outputFormatClass;
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param inputFormatClass  of type Class<? extends DBInputFormat>
   * @param outputFormatClass of type Class<? extends DBOutputFormat>
   * @param columns           of type String[]
   * @param orderBy           of type String[]
   * @param conditions        of type String
   * @param updateBy          of type String[]
   */
  public JDBCScheme( Class<? extends DBInputFormat> inputFormatClass, Class<? extends DBOutputFormat> outputFormatClass, String[] columns, String[] orderBy, String conditions, String[] updateBy )
    {
    this( inputFormatClass, outputFormatClass, columns, orderBy, conditions, -1, updateBy );
    }

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
    this( inputFormatClass, outputFormatClass, columns, orderBy, null, updateBy );
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
   * @param columns    of type String[]
   * @param orderBy    of type String[]
   * @param conditions of type String
   * @param limit      of type long
   */
  public JDBCScheme( String[] columns, String[] orderBy, String conditions, long limit )
    {
    this( null, null, columns, orderBy, conditions, limit, null );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns    of type String[]
   * @param orderBy    of type String[]
   * @param conditions of type String
   */
  public JDBCScheme( String[] columns, String[] orderBy, String conditions )
    {
    this( null, null, columns, orderBy, conditions, null );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   * @param orderBy of type String[]
   * @param limit   of type long
   */
  public JDBCScheme( String[] columns, String[] orderBy, long limit )
    {
    this( null, null, columns, orderBy, null, limit, null );
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
   * @param columns    of type String[]
   * @param conditions of type String
   * @param limit      of type long
   */
  public JDBCScheme( String[] columns, String conditions, long limit )
    {
    this( null, null, columns, null, conditions, limit, null );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns    of type String[]
   * @param conditions of type String
   */
  public JDBCScheme( String[] columns, String conditions )
    {
    this( null, null, columns, null, conditions, null );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   *
   * @param columns of type String[]
   * @param limit   of type long
   */
  public JDBCScheme( String[] columns, long limit )
    {
    this( null, null, columns, null, null, limit, null );
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

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   * <p/>
   * Use this constructor if the data source may only be used as a source.
   *
   * @param inputFormatClass of type Class<? extends DBInputFormat>
   * @param columns          of type String[]
   * @param selectQuery      of type String
   * @param countQuery       of type String
   * @param limit            of type long
   */
  public JDBCScheme( Class<? extends DBInputFormat> inputFormatClass, String[] columns, String selectQuery, String countQuery, long limit )
    {
    this.columnFields = new Fields( columns );

    setSourceFields( columnFields );

    this.columns = columns;
    this.selectQuery = selectQuery.trim().replaceAll( ";$", "" );
    this.countQuery = countQuery.trim().replaceAll( ";$", "" );
    this.limit = limit;

    this.inputFormatClass = inputFormatClass;
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   * <p/>
   * Use this constructor if the data source may only be used as a source.
   *
   * @param columns     of type String[]
   * @param selectQuery of type String
   * @param countQuery  of type String
   * @param limit       of type long
   */
  public JDBCScheme( String[] columns, String selectQuery, String countQuery, long limit )
    {
    this( null, columns, selectQuery, countQuery, limit );
    }

  /**
   * Constructor JDBCScheme creates a new JDBCScheme instance.
   * <p/>
   * Use this constructor if the data source may only be used as a source.
   *
   * @param columns     of type String[]
   * @param selectQuery of type String
   * @param countQuery  of type String
   */
  public JDBCScheme( String[] columns, String selectQuery, String countQuery )
    {
    this( null, columns, selectQuery, countQuery, -1 );
    }


  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    if( selectQuery != null )
      {
      DBInputFormat.setInput( conf, TupleRecord.class, selectQuery, countQuery, limit );
      }
    else
      {
      String tableName = ( (JDBCTap) tap ).getTableName();
      String joinedOrderBy = orderBy != null ? Util.join( orderBy, ", " ) : null;
      DBInputFormat.setInput( conf, TupleRecord.class, tableName, conditions, joinedOrderBy, limit, columns );
      }

    if( inputFormatClass != null )
      conf.setInputFormat( inputFormatClass );
    }

  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    if( selectQuery != null )
      throw new TapException( "cannot sink to this Scheme" );

    String tableName = ( (JDBCTap) tap ).getTableName();
    int batchSize = ( (JDBCTap) tap ).getBatchSize();
    DBOutputFormat.setOutput( conf, DBOutputFormat.class, tableName, columns, updateBy, batchSize );

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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof JDBCScheme ) )
      return false;
    if( !super.equals( object ) )
      return false;

    JDBCScheme that = (JDBCScheme) object;

    if( limit != that.limit )
      return false;
    if( columnFields != null ? !columnFields.equals( that.columnFields ) : that.columnFields != null )
      return false;
    if( !Arrays.equals( columns, that.columns ) )
      return false;
    if( conditions != null ? !conditions.equals( that.conditions ) : that.conditions != null )
      return false;
    if( countQuery != null ? !countQuery.equals( that.countQuery ) : that.countQuery != null )
      return false;
    if( inputFormatClass != null ? !inputFormatClass.equals( that.inputFormatClass ) : that.inputFormatClass != null )
      return false;
    if( !Arrays.equals( orderBy, that.orderBy ) )
      return false;
    if( outputFormatClass != null ? !outputFormatClass.equals( that.outputFormatClass ) : that.outputFormatClass != null )
      return false;
    if( selectQuery != null ? !selectQuery.equals( that.selectQuery ) : that.selectQuery != null )
      return false;
    if( !Arrays.equals( updateBy, that.updateBy ) )
      return false;
    if( updateByFields != null ? !updateByFields.equals( that.updateByFields ) : that.updateByFields != null )
      return false;
    if( updateIfTuple != null ? !updateIfTuple.equals( that.updateIfTuple ) : that.updateIfTuple != null )
      return false;
    if( updateValueFields != null ? !updateValueFields.equals( that.updateValueFields ) : that.updateValueFields != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( inputFormatClass != null ? inputFormatClass.hashCode() : 0 );
    result = 31 * result + ( outputFormatClass != null ? outputFormatClass.hashCode() : 0 );
    result = 31 * result + ( columns != null ? Arrays.hashCode( columns ) : 0 );
    result = 31 * result + ( orderBy != null ? Arrays.hashCode( orderBy ) : 0 );
    result = 31 * result + ( conditions != null ? conditions.hashCode() : 0 );
    result = 31 * result + ( updateBy != null ? Arrays.hashCode( updateBy ) : 0 );
    result = 31 * result + ( updateValueFields != null ? updateValueFields.hashCode() : 0 );
    result = 31 * result + ( updateByFields != null ? updateByFields.hashCode() : 0 );
    result = 31 * result + ( columnFields != null ? columnFields.hashCode() : 0 );
    result = 31 * result + ( updateIfTuple != null ? updateIfTuple.hashCode() : 0 );
    result = 31 * result + ( selectQuery != null ? selectQuery.hashCode() : 0 );
    result = 31 * result + ( countQuery != null ? countQuery.hashCode() : 0 );
    result = 31 * result + (int) ( limit ^ ( limit >>> 32 ) );
    return result;
    }
  }
