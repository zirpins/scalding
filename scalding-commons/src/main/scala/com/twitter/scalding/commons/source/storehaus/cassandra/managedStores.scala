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

import org.apache.hadoop.io.Writable
import com.twitter.storehaus.cascading.split.SplittableStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.WritableStore
import com.twitter.storehaus.Store

/**
 * Version store extension offering some management functionality
 */
trait ManagedVersionedStore {

  /**
   * return latest version within the version store
   */
  def lastVersion(): Long

  /**
   * return latest version within the version store
   */
  def versions(): Iterable[Long]

  /**
   * return latest version in store before
   */
  def lastVersionBefore(version: Long): Option[Long]
}

/**
 * Generic abstraction of cassandra management functions for splittable stores
 */
trait ManagedCassandraStore[KeyT, ValT] {

  /**
   *  some implementation to create store specific column families
   */
  def createColumnFamily(cf: StoreColumnFamily): Unit

  /**
   *  some implementation to create store instances
   */
  def createStore(cf: StoreColumnFamily): Store[KeyT, ValT]
}
