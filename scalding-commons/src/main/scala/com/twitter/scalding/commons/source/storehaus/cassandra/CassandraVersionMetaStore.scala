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

import com.twitter.scalding.commons.source.storehaus.ManagedCassandraStore
import com.twitter.scalding.commons.source.storehaus.ManagedVersionedStore
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.WritableStore
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import com.twitter.storehaus.cassandra.cql.CASStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.storehaus.cassandra.cql.CQLCassandraStore
import com.twitter.storehaus.cassandra.cql.CassandraCASStore
import com.twitter.storehaus.cassandra.cql.TokenFactory.longTokenFactory
import com.twitter.util.Await
import com.twitter.util.Time
import com.websudos.phantom.CassandraPrimitive

/**
 * Default cassandra version meta store based on a simple underlying
 */
object CassandraVersionMetaStore {
  def apply(versionstoreId: String, storeSession: StoreSession): CassandraVersionMetaStore = {
    val metaStoreCF = StoreColumnFamily(versionstoreId + METATSTORE_CF_SUFFIX, storeSession)
    CQLCassandraStore.createColumnFamilyWithToken[Long, String, Long](metaStoreCF, Some(implicitly[CassandraPrimitive[Long]]))
    val store = new CQLCassandraStore[Long, String](metaStoreCF)
    new CassandraVersionMetaStore(store.getCASStore[Long]())
  }
}

/**
 * Enumeration of possible version states:
 * - Prepare: the version store is under preparation (i.e. the column family is being created)
 * - Valid: the version store exists and can be used (i.e. the column family exists)
 * - Invalid: the version store had existed but was invalidated and can not be used anymore (i.e. the column family has been dropped)
 */
object VersionState extends Enumeration {
  type VersionState = Value
  val Prepare, Valid, Invalid = Value
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
  def close(timeout: Time): Unit = underlying.close(timeout)

  /**
   * prepare version store, succeeds if
   *  - version did not exist
   *  AND
   *  - no concurrent thread has changed the value.
   *  In case of success, it is save to create a respective version store.
   */

  def prepareValidateNoRewrite(version: Long): Boolean =
    Await.result(underlying.cas(None, (version, VersionState.Prepare.toString)))

  /**
   * prepare version store, succeeds if
   *  - version did not exist OR was invalid before
   *  AND
   *  - no concurrent thread has changed the value.
   *  In case of success, it is save to create a respective version store.
   */
  def prepareValidate(version: Long): Boolean = Await.result(underlying.get(version)).map {
    _ match {
      case (v, t) if (v.equals(VersionState.Invalid.toString)) =>
        Await.result(underlying.cas(Some(t), (version, VersionState.Prepare.toString)))
      case _ => false // can only validate from invalid state...
    }
  }.getOrElse(Await.result(underlying.cas(None, (version, VersionState.Prepare.toString)))) // ...or no state at all

  /**
   * release version store after preparation
   */
  def validate(version: Long): Boolean = Await.result(underlying.get(version)).map {
    _ match {
      case (v, t) if (v.equals(VersionState.Prepare.toString)) =>
        Await.result(underlying.cas(Some(t), (version, VersionState.Valid.toString)))
      case _ => false // can only validate from prepare state
    }
  }.getOrElse(false) // no state at all

  /**
   * mark version as invalid, succeeds if
   *  - version was valid before
   *  AND
   *  - no concurrent thread has changed the value.
   *  In case of success, it is save to drop a respective version store
   */
  def invalidate(version: Long): Boolean = Await.result(underlying.get(version)).map {
    _ match {
      case (v, t) if (v.equals(VersionState.Valid.toString)) =>
        Await.result(underlying.cas(Some(t), (version, VersionState.Invalid.toString)))
      case _ => false // can only invalidate from valid state
    }
  }.getOrElse(false) // no state at all

  /**
   * remove version from store
   *
   * Not yet implemented
   */
  def remove(version: Long): Unit = ???

  /**
   * read all version info
   */
  private def readAll: Seq[(Long, String)] = Await.result(Await.result(underlying getAll).toSeq)

  /**
   * return all versions
   */
  def allVersTuples: Seq[(Long, String)] = readAll

  /**
   * return all versions marked as valid
   */
  def vers: Seq[Long] = allVersTuples.filter { a => a._2.equals(VersionState.Valid.toString) }.map { _._1 }

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
  def latestVer: Option[Long] = vers match {
    case vers if !vers.isEmpty => Some(vers.sorted.last)
    case _ => None
  }

}
