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

import scala.collection.JavaConversions._
import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.Store
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.storehaus.JMapStore
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import org.apache.hadoop.mapred.JobConf

/**
 * Specification of the VersionedCassandraStoreInitializer
 */
class VersionedCassandraStoreInitializerSpec extends Specification with Mockito {

  /**
   * An extended JMaPStore implementing IterableStore
   */
  class IterableJMapStore[K, V]() extends JMapStore[K, V]() with IterableStore[K, V] {
    override def getAll: Future[Spool[(K, V)]] =
      IterableStore.iteratorToSpool(
        jstore.filter { t: (K, Option[V]) => t._2.nonEmpty }.
          map { t: (K, Option[V]) => (t._1, t._2.get) }.iterator)
  }

  /**
   * MetaStoreT in-memory implementation
   */
  class MetaStoreImpl extends IterableJMapStore[Long, Boolean]()

  /**
   * Mock Initializer implementation
   */
  class TestInitilizer(metaStore: MetaStoreT) extends VersionedCassandraStoreInitializer[String, String](
    3, Some(metaStore)) {
    val storeSessionMock = mock[StoreSession]
    val storeMock = mock[Store[String, String]]

    override def getStoreSession: StoreSession = storeSessionMock
    override def createColumnFamily(cf: StoreColumnFamily): Unit = { /* ignored */ }
    override def createStore(cf: StoreColumnFamily): Store[String, String] = storeMock
  }

  val jobConfMock = mock[JobConf]

  "A VersionedCassandraStoreInitializer" should {

    "Return -1 as version of an empty store" in {
      val init = new TestInitilizer(new MetaStoreImpl())
      init.lastVersion() must_== -1L
    }

    "Keep track of the latest version" in {
      val init = new TestInitilizer(new MetaStoreImpl())

      // version properly initialized
      init.getWritableStore(jobConfMock, 1L)
      init.lastVersion() must_== 1L

      // version update considered
      init.getWritableStore(jobConfMock, 2L)
      init.lastVersion() must_== 2L
    }

    "Restrict the maximum number of versions" in {
      val init = new TestInitilizer(new MetaStoreImpl())
      init.getWritableStore(jobConfMock, 1L)
      init.getWritableStore(jobConfMock, 2L)
      init.getWritableStore(jobConfMock, 3L)
      init.getWritableStore(jobConfMock, 4L)

      // version 1 should have been dropped
      init.versions.size must_== 3
    }

    "Disallow writing only for outdated versions" in {
      val init = new TestInitilizer(new MetaStoreImpl())
      init.getWritableStore(jobConfMock, 1L)
      init.getWritableStore(jobConfMock, 2L)
      init.getWritableStore(jobConfMock, 3L)
      init.getWritableStore(jobConfMock, 4L)

      // allow writes to older valid versions
      init.getWritableStore(jobConfMock, 2L).nonEmpty must_== true
      init.getWritableStore(jobConfMock, 3L).nonEmpty must_== true
      init.getWritableStore(jobConfMock, 4L).nonEmpty must_== true

      // disallow outdated writes 
      init.getWritableStore(jobConfMock, 1L).isEmpty must_== true
    }

    "Find (only) managed versions" in {
      val init = new TestInitilizer(new MetaStoreImpl())
      init.getWritableStore(jobConfMock, 1L)
      init.getWritableStore(jobConfMock, 2L)
      init.getWritableStore(jobConfMock, 3L)
      init.getWritableStore(jobConfMock, 4L)

      // find before should exists
      init.lastVersionBefore(4) must_== Some(3L)

      // find before shouldn't exist
      init.lastVersionBefore(2) must_== None
    }

    "Allow reading only for valid versions" in {
      val init = new TestInitilizer(new MetaStoreImpl())
      init.getWritableStore(jobConfMock, 1L)
      init.getWritableStore(jobConfMock, 2L)
      init.getWritableStore(jobConfMock, 3L)
      init.getWritableStore(jobConfMock, 4L)

      // allow reads of valid versions 
      init.getReadableStore(jobConfMock, 2L).nonEmpty must_== true
      init.getReadableStore(jobConfMock, 3L).nonEmpty must_== true
      init.getReadableStore(jobConfMock, 4L).nonEmpty must_== true

      // disallow outdated reads
      init.getReadableStore(jobConfMock, 1L).isEmpty must_== true
    }

  }
}
