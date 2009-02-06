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

import java.io.Serializable;
import java.util.Arrays;

import cascading.util.Util;

/**
 * Class TableDesc describes a SQL based table, this description is used by the {@link JDBCTap} when
 * creating a missing table.
 *
 * @see JDBCTap
 * @see JDBCScheme
 */
public class TableDesc implements Serializable
  {
  /** Field tableName  */
  String tableName;
  /** Field columnNames  */
  String[] columnNames;
  /** Field columnDefs  */
  String[] columnDefs;
  /** Field primaryKey  */
  String primaryKey;

  /**
   * Constructor TableDesc creates a new TableDesc instance.
   *
   * @param tableName of type String
   * @param columnNames of type String[]
   */
  public TableDesc( String tableName, String[] columnNames )
    {
    this.tableName = tableName;
    this.columnNames = columnNames;
    }

  /**
   * Constructor TableDesc creates a new TableDesc instance.
   *
   * @param tableName of type String
   * @param columnNames of type String[]
   * @param columnDefs of type String[]
   * @param primaryKey of type String
   */
  public TableDesc( String tableName, String[] columnNames, String[] columnDefs, String primaryKey )
    {
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnDefs = columnDefs;
    this.primaryKey = primaryKey;
    }

  /**
   * Method getTableCreateStatement returns the tableCreateStatement of this TableDesc object.
   *
   * @return the tableCreateStatement (type String) of this TableDesc object.
   */
  public String getTableCreateStatement()
    {
    String[] decl = new String[columnNames.length + ( hasPrimaryKey() ? 1 : 0 )];

    for( int i = 0; i < columnNames.length; i++ )
      {
      String columnName = columnNames[ i ];
      String columnDef = columnDefs[ i ];

      decl[ i ] = columnName + " " + columnDef;
      }

    if( hasPrimaryKey() )
      decl[ decl.length - 1 ] = String.format( "PRIMARY KEY( %s )", primaryKey );

    return String.format( "CREATE TABLE %s ( %s )", tableName, Util.join( decl, ", " ) );
    }

  /**
   * Method getTableDropStatement returns the tableDropStatement of this TableDesc object.
   *
   * @return the tableDropStatement (type String) of this TableDesc object.
   */
  public String getTableDropStatement()
    {
    return String.format( "DROP TABLE %s", tableName );
    }

  /**
   * Method getTableExistsQuery returns the tableExistsQuery of this TableDesc object.
   *
   * @return the tableExistsQuery (type String) of this TableDesc object.
   */
  public String getTableExistsQuery()
    {
    return String.format( "select 1 from %s where 1 = 0", tableName );
    }

  private boolean hasPrimaryKey()
    {
    return primaryKey != null && primaryKey.length() != 0;
    }

  @Override
  public String toString()
    {
    return "TableDesc{" + "tableName='" + tableName + '\'' + ", columnNames=" + ( columnNames == null ? null : Arrays.asList( columnNames ) ) + ", primaryKey='" + primaryKey + '\'' + '}';
    }
  }
