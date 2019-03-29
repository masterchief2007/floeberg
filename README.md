# floeberg
Experiments with Apache Iceberg

Apache Iceberg can be referenced at http://iceberg.incubator.apache.org/

Parquet format is available at https://github.com/apache/parquet-format
and Parquet-MR is the java implementation and is available at https://github.com/apache/parquet-mr .

### Objective
 Create hello-world with Apache Iceberg and Parquet.


#### Pre-requisites

Build a local version of Iceberg using gradle as specified at https://github.com/apache/incubator-iceberg



#### References
0. https://www.youtube.com/watch?v=D0vd325CqoM - Introducing Iceberg tables designed for object stores, by Ryan Blue.
1. Iceberg table specification - https://docs.google.com/document/d/1Q-zL5lSCle6NEEdyfiYsXYzX_Q8Qf0ctMyGBKslOswA
2. [https://gradle.org/][Gradle]
2. https://github.com/apache/incubator-iceberg
3. https://github.com/apache/parquet-mr

[Gradle]: https://gradle.org/

#### Note
1. After including dependency on Spark 2.4 got error on  incompatible Jackson version 2.7.8
com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.7.8

   upgraded to jackson 2.9.8
   Refer to https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
2.