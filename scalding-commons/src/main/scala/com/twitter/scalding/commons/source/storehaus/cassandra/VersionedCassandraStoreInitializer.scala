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
import com.twitter.util.Try
import java.util.concurrent.TimeUnit

/**
 * Simple implementation of versioning for VersionedStoreCascadingInitializer
 * mapping versions to cassandra column families.
 *
 * @param identifier to distinguish multiple versioned stores (not needed with single stores)
 * @param versionsToKeep the store keeps up to this number of successive versions. Older versions will not be deleted but ignored.
 * @param metaStore an optional custom meta store. If this is omitted, a default meta store based on cassandra will be used
 */
abstract class VersionedCassandraStoreInitializer[KeyT, ValT](
  val identifier: String = DEFAULT_VERSIONSTORE_IDENTIFIER,
  val versionsToKeep: Int = DEFAULT_VERSIONS_TO_KEEP,
  val metaStoreUnderlying: Option[MetaStoreUnderlyingT] = None)
  extends VersionedStorehausCascadingInitializer[KeyT, ValT]
  with ManagedVersionedStore
  with ManagedCassandraStore[KeyT, ValT]
  with CassandraConfig {

  @transient private val logger = LoggerFactory.getLogger(
    classOf[VersionedCassandraStoreInitializer[KeyT, ValT]])

  val metaStore = metaStoreUnderlying match {
    case Some(store) => new CassandraVersionMetaStore(store)
    case None => CassandraVersionMetaStore(identifier, getStoreSession)
  }

  /**
   * Shutdown the versioned store initializer
   */
  override def close = { metaStore.close(Time.now) }

  /**
   * Meta store operation implementing {@link ManagedVersionedStore.lastVersion}
   */
  override def lastVersion(): Long = { metaStore.latestVer }

  /**
   * Meta store operation implementing {@link ManagedVersionedStore.versions}
   */
  override def versions(): Iterable[Long] = { metaStore.vers }

  /**
   * Meta store operation implementing {@link ManagedVersionedStore.lastVersionBefore}
   */
  override def lastVersionBefore(version: Long): Option[Long] = {
    metaStore.vers.filter { _ < version }.reduceOption { (a, b) => if (a < b) b else a };
  }

  /**
   * dynamically create StoreColumnFamily instance of a versioned store
   */
  protected def getCf(ver: Long): StoreColumnFamily = {
    StoreColumnFamily(getCFBaseName + ver, getStoreSession);
  }

  /**
   *  prepare version store
   */
  override def prepareStore(version: Long): Boolean = {

    // drop oldest version if max number is reached
    if (!((versionsToKeep - metaStore.numVers) > 0)) {
      val oldver = metaStore.vers.sorted.head
      if (metaStore.invalidate(oldver)) {
        logger.info(s"Dropping old version store '$oldver'.")
        getCf(oldver).dropAndDeleteColumnFamilyAndContainedData
      } else {
        logger.warn(s"Invalidation of version store '$version' revoked due to conflict.")
      }
    }

    val VALIDATE_WAIT = 3 * 1000 // 3 sec
    val VALIDATE_TIMEOUT = 5 * 60 * 1000 // 5 min

    // create new version store
    if (metaStore.prepareValidate(version)) {
      logger.info(s"Creating new version store '$version'.")
      createColumnFamily(getCf(version))
      metaStore.validate(version)
    } else {
      logger.info(s"Awaiting validation of version store '$version'.")
      val startTime = System.currentTimeMillis()
      while (!metaStore.hasVer(version)) {
        TimeUnit.MILLISECONDS.sleep(VALIDATE_WAIT)
        if (System.currentTimeMillis() - startTime > VALIDATE_TIMEOUT) {
          logger.error(s"Unable to prepare version store '$version': validation timeout.")
          return false
        }
      }
    }

    true;
  }

  /**
   * Retrieves some readable store for a version if existing, otherwise returns none.
   */
  override def getReadableStore(jobConf: JobConf, version: Long): Option[ReadableStore[KeyT, ValT]] = {
    metaStore.hasVer(version) match {
      case true => {
        Some(createReadableStore(getCf(version)).asInstanceOf[ReadableStore[KeyT, ValT]]);
      }
      case false => {
        logger.error("Tried to retrieve non-existing version store for reading.");
        None;
      }
    }
  }

  // create specific store instance
  private def getWritableStoreOnce(version: Long): WritableStore[KeyT, Option[ValT]] = {
    createWritableStore(getCf(version)).asInstanceOf[WritableStore[KeyT, Option[ValT]]];
  };

  /**
   * Retrieves a writable store for a version. If the version does not already exist
   * a new physical store will be created. If the maximum number of versions is exceeded
   * the last existing version will be dropped.
   */
  override def getWritableStore(jobConf: JobConf, version: Long): Option[WritableStore[KeyT, Option[ValT]]] = {
    metaStore.hasVer(version) match {
      case true => {
        Some(getWritableStoreOnce(version));
      }
      case false => {
        // make sure new version is ahead
        if (metaStore.latestVer > version) {
          logger.error(s"Tried to retrieve outdated non-existing version store '$version' for writing.");
          return None;
        }
        // prepare version store
        if (prepareStore(version)) Some(getWritableStoreOnce(version)) else None;
      }
    }
  }
}
