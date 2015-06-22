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

import scala.util.control.Breaks._
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

  // timeouts for concurrent operations
  val VALIDATE_WAIT = 3 * 1000 // 3 sec
  val VALIDATE_TIMEOUT = 5 * 60 * 1000 // 5 min

  /**
   * Shutdown the versioned store initializer
   */
  override def close(timeout: Time) = { metaStore.close(timeout) }

  /**
   * Meta store operation implementing {@link ManagedVersionedStore.lastVersion}
   */
  override def lastVersion(): Option[Long] = { metaStore.latestVer }

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

    // try to obtain a lock for preparing the version store
    if (metaStore.prepareValidate(version)) {

      // we are the first/exclusive to prepare this version store

      logger.info(s"Creating new version store '$version'.")
      createColumnFamily(getCf(version))
      metaStore.validate(version)

      // This is a good time for cleaning up outdated versions.

      // Drop oldest versions if max number is reached. If something went wrong, 
      // in the past, there might be even multiple outdated versions.
      while (metaStore.numVers > versionsToKeep) dropVersion(metaStore.vers.sorted.head)

    } else { // we failed to obtain the lock

      // the version store is being prepared by somebody else
      // so we wait until this operation has been finished

      logger.info(s"Awaiting validation of version store '$version'.")

      val startTimeCreate = System.currentTimeMillis()

      breakable {
        while (!metaStore.hasVer(version)) {
          // Performs a Thread.sleep using this time unit.
          TimeUnit.MILLISECONDS.sleep(VALIDATE_WAIT)
          if (System.currentTimeMillis() - startTimeCreate > VALIDATE_TIMEOUT) {
            logger.error(s"Unable to prepare version store '$version': validation timeout.")
            break // no point to wait any longer - good luck
          }
        }
      }
    }

    true;
  }

  /**
   * Invalidate version and drop associated table. The function considers concurrent
   * invalidation and tries to ensure that the version was really dropped. It will
   * however give up after a timeout.
   */
  private def dropVersion(version: Long) = {
    val startTimeDrop = System.currentTimeMillis()
    if (metaStore.invalidate(version)) {
      logger.info(s"Dropping old version store '$version'.")
      getCf(version).dropAndDeleteColumnFamilyAndContainedData
    } else {
      breakable {
        logger.warn(s"Invalidation of version store '$version' revoked due to conflict. " +
          "Waiting for concurrent invalidation to succeed.")
        while (metaStore.hasVer(version)) {
          if (System.currentTimeMillis() - startTimeDrop > VALIDATE_TIMEOUT) {
            logger.warn("Unable to drop all outdated version stores due to timeout.")
            break // no point to wait any longer - good luck
          } else {
            // wait for competitor to finish drop activity
            TimeUnit.MILLISECONDS.sleep(VALIDATE_WAIT)
          }
        }
      }
    }
  }

  /**
   * clear all versions in store after upperBound (useful for re-writing versions).
   * This function is not fully guarded against concurrent activities!
   */
  override def resetVersions(upperBound: Long): Unit =
    versions.filter(_ > upperBound).foreach(dropVersion(_))

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
        metaStore.latestVer.map { ver =>
          if (ver > version) {
            logger.error(s"Tried to retrieve outdated non-existing version store '$version' for writing.");
            return None;
          }
        }
        // prepare version store
        if (prepareStore(version)) Some(getWritableStoreOnce(version)) else None;
      }
    }
  }
}
