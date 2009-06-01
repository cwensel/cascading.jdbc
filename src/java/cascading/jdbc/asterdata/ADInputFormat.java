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

import java.sql.Connection;

import cascading.jdbc.db.DBInputFormat;

/** This DBInputFormat subclass simply disables the transaction isolation level setting. */
public class ADInputFormat extends DBInputFormat
  {
  public ADInputFormat()
    {
    }

  @Override
  protected void setTransactionIsolationLevel( Connection connection )
    {
    // do nothing
    }
  }
