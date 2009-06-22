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

import cascading.jdbc.JDBCScheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class AsterDataScheme extends JDBCScheme
  {
  public AsterDataScheme( String[] columns, String[] orderBy, String conditions, String[] updateBy )
    {
    super( ADInputFormat.class, null, columns, orderBy, conditions, updateBy );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String[] orderBy, String conditions, Fields updateByFields, String[] updateBy )
    {
    super( ADInputFormat.class, null, columnFields, columns, orderBy, conditions, updateByFields, updateBy );
    }

  public AsterDataScheme( String[] columns, String[] orderBy, String conditions, long limit )
    {
    super( ADInputFormat.class, null, columns, orderBy, conditions, limit, null );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String[] orderBy, String conditions, long limit )
    {
    super( ADInputFormat.class, null, columnFields, columns, orderBy, conditions, limit, null, null );
    }

  public AsterDataScheme( String[] columns, String[] orderBy, String conditions )
    {
    super( ADInputFormat.class, null, columns, orderBy, conditions, null );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String[] orderBy, String conditions )
    {
    super( ADInputFormat.class, null, columnFields, columns, orderBy, conditions, null, null );
    }

  public AsterDataScheme( String[] columns, String[] orderBy, String[] updateBy )
    {
    super( ADInputFormat.class, null, columns, orderBy, updateBy );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String[] orderBy, Fields updateByFields, String[] updateBy )
    {
    super( ADInputFormat.class, null, columnFields, columns, orderBy, updateByFields, updateBy );
    }

  public AsterDataScheme( String[] columns, String[] orderBy )
    {
    this( columns, orderBy, null, null );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String[] orderBy )
    {
    this( columnFields, columns, orderBy, null, null );
    }

  public AsterDataScheme( String[] columns, long limit )
    {
    this( columns, (String[]) null, null, limit );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, long limit )
    {
    this( columnFields, columns, (String[]) null, null, limit );
    }

  public AsterDataScheme( String[] columns )
    {
    this( columns, null, null, null );
    }

  public AsterDataScheme( Fields columnFields, String[] columns )
    {
    this( columnFields, columns, null, null, null );
    }

  public AsterDataScheme( String[] columns, String selectQuery, String countQuery )
    {
    super( ADInputFormat.class, columns, selectQuery, countQuery, -1 );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String selectQuery, String countQuery )
    {
    super( ADInputFormat.class, columnFields, columns, selectQuery, countQuery, -1 );
    }

  public AsterDataScheme( String[] columns, String selectQuery, String countQuery, long limit )
    {
    super( ADInputFormat.class, columns, selectQuery, countQuery, limit );
    }

  public AsterDataScheme( Fields columnFields, String[] columns, String selectQuery, String countQuery, long limit )
    {
    super( ADInputFormat.class, columnFields, columns, selectQuery, countQuery, limit );
    }

  @Override
  protected Tuple cleanTuple( Tuple result )
    {
    for( int i = 0; i < result.size(); i++ )
      {
      Comparable value = result.get( i );

      if( value instanceof String )
        {
        result.set( i, ( (String) value ).replaceAll( "'", "''" ) );
        result.set( i, ( (String) value ).replaceAll( "\n", "\\n" ) );
        }
      }

    return result;
    }
  }
