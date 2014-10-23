/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.commons.source.storehaus.cassandra

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.websudos.phantom.CassandraPrimitive

/**
 * Simple abstraction of local cassandra config params
 */
trait CassandraConfig {

  /**
   * Column family base name.
   */
  def getCFBaseName = "versionedtable"

  def getStoreSession: StoreSession

}

/**
 * Simple abstraction of custom cassandra tuple store set up
 */
trait CassandraTupleStoreConfig[RKT <: Product, CKT <: Product, ValueT] {

  def getThriftConnections: String

  def colkeyColumnNames: List[String]

  def rowkeyColumnNames: List[String]

  def valueSerializer: CassandraPrimitive[ValueT]
}
