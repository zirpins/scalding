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

import org.slf4j.LoggerFactory
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.Writable
import com.twitter.util.Await
import com.twitter.storehaus.{ ReadableStore, WritableStore }
import com.twitter.storehaus.cassandra.cql.CQLCassandraStore
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.scalding.commons.source.storehaus.{ ManagedVersionedStore, ManagedCassandraStore }
import com.twitter.util.Time

/**
 * Default cassandra version meta store based on a simple underlying
 */
object CassandraVersionMetaStore {
  def apply(versionstoreId: String, storeSession: StoreSession): CassandraVersionMetaStore = {
    val metaStoreCF = StoreColumnFamily(versionstoreId + METATSTORE_CF_SUFFIX, storeSession)
    CQLCassandraStore.createColumnFamily[Long, Boolean](metaStoreCF)
    val store = new CQLCassandraStore[Long, Boolean](metaStoreCF)
    new CassandraVersionMetaStore(store)
  }
}

/**
 * Meta store for version housekeeping of VersionedCassandraStoreInitializer
 *
 * @param storeSession cassandra session to access the underlying cassandra store
 * @param underlying storehaus store to be used for storing version data
 *
 */
class CassandraVersionMetaStore(val underlying: MetaStoreUnderlyingT) {

  /**
   * close meta store
   */
  def close(deadline: Time): Unit = underlying.close(deadline)

  /**
   * mark version as valid
   */
  def validate(version: Long): Unit = underlying.put(version, Some(true))

  /**
   * mark version as invalid
   */
  def invalidate(version: Long): Unit = underlying.put(version, Some(false))

  /**
   * remove version from store
   *
   * Not yet implemented
   */
  def remove(version: Long): Unit = ???

  /*
     * read all version info
     */
  private def readAll: Seq[(Long, Boolean)] = Await.result(Await.result(underlying getAll).toSeq)

  /**
   * return all versions
   */
  def allVersTuples: Seq[(Long, Boolean)] = readAll

  /**
   * return all versions marked as valid
   */
  def vers: Seq[Long] = allVersTuples.filter { a => a._2 }.map { _._1 }

  /**
   * count valid versions
   */
  def numVers: Long = vers.count { _ => true }

  /**
   * check validity of version
   */
  def hasVer(ver: Long): Boolean = vers.count { _.equals(ver) } == 1

  /**
   * return latest valid version
   */
  def latestVer: Long = {
    val v = vers;
    if (v.isEmpty) -1 else v.sorted.last;
  }

}
