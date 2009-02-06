/*
 * Copyright (c) 2009, Your Corporation. All Rights Reserved.
 */

package cascading.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import org.apache.hadoop.mapred.lib.db.DBWritable;
import cascading.tuple.Tuple;

/**
 *
 */
public class TupleRecord implements DBWritable
  {
  private Tuple tuple;

  public TupleRecord()
    {
    }

  public TupleRecord( Tuple tuple )
    {

    this.tuple = tuple;
    }

  public void setTuple( Tuple tuple )
    {
    this.tuple = tuple;
    }

  public Tuple getTuple()
    {
    return tuple;
    }

  public void write( PreparedStatement statement ) throws SQLException
    {
    for( int i = 0; i < tuple.size(); i++ )
      statement.setObject( i + 1, tuple.get( i ) );
    }

  public void readFields( ResultSet resultSet ) throws SQLException
    {
    tuple = new Tuple();

    for( int i = 0; i < resultSet.getMetaData().getColumnCount(); i++ )
      tuple.add( (Comparable) resultSet.getObject( i + 1 ) );
    }

  }
