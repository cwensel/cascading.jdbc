
Welcome

 This is the Cascading.JDBC module.

 It provides support for reading/writing data to/from an RDBMS via
 JDBC drivers when bound to a Cascading data processing flow.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/

Building

 This release requires at least Cascading 1.0.1. Hadoop 0.19.x,
 and the related HBase release.

 To build a jar,

 > ant -Dcascading.home=... -Dhadoop.home=... jar

 To test,

 > ant -Dcascading.home=... -Dhadoop.home=... test

where "..." is the install path of each of the dependencies.


Using

  The cascading-jdbc.jar file should be added to the "lib"
  directory of your Hadoop application jar file along with all
  Cascading dependencies.

  See the JDBCTest unit test for sample code on using the JDBC taps and
  schemes in your Cascading application.

License

  Copyright (c) 2009 Concurrent, Inc.

  This work has been released into the public domain
  by the copyright holder. This applies worldwide.

  In case this is not legally possible:
  The copyright holder grants any entity the right
  to use this work for any purpose, without any
  conditions, unless such conditions are required by law.