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

import java.util.List;

import cascading.jdbc.TableDesc;

/**
 *
 */
public class AsterDataTableDesc extends TableDesc
  {
  /** Field partitionKey */
  protected String partitionKey;

  public AsterDataTableDesc( String tableName )
    {
    super( tableName );
    }

  public AsterDataTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys )
    {
    super( tableName, columnNames, columnDefs, primaryKeys );
    }

  public AsterDataTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys, String partitionKey )
    {
    super( tableName, columnNames, columnDefs, primaryKeys );
    this.partitionKey = partitionKey;
    }

  @Override
  protected List<String> addCreateTableBodyTo( List<String> createTableStatement )
    {
    createTableStatement = super.addCreateTableBodyTo( createTableStatement );

    if( hasPartitionKey() )
      createTableStatement.add( String.format( "PARTITION KEY( %s )", partitionKey ) );

    return createTableStatement;
    }

  private boolean hasPartitionKey()
    {
    return partitionKey != null && partitionKey.length() != 0;
    }
  }
