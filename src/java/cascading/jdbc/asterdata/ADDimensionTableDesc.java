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

package cascading.jdbc.asterdata;

/**
 *
 */
public class ADDimensionTableDesc extends AsterDataTableDesc
  {
  public ADDimensionTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys, String partitionKey )
    {
    super( tableName, columnNames, columnDefs, primaryKeys, partitionKey );
    }

  @Override
  protected String getCreateTableFormat()
    {
    return "CREATE DIMENSION TABLE %s ( %s )";
    }

  @Override
  protected String getDropTableFormat()
    {
    return "DROP TABLE %s CASCADE";
    }
  }
