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

import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import shapeless._
import shapeless.ops.hlist._
import shapeless.UnaryTCConstraint._
import com.websudos.phantom.CassandraPrimitive
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.querybuilder.Clause
import org.apache.hadoop.mapred.JobConf
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.Store
import com.twitter.storehaus.cascading.StorehausInputFormat
import com.twitter.storehaus.cascading.split.{
  SplittableStore,
  StorehausSplittingMechanism
}
import com.twitter.storehaus.cassandra.cql.{
  AbstractCQLCassandraCompositeStore,
  CQLCassandraCollectionStore,
  CQLCassandraConfiguration,
  CassandraTupleStore
}
import com.twitter.storehaus.cassandra.cql.macrobug._
import CQLCassandraConfiguration._
import AbstractCQLCassandraCompositeStore._
import com.twitter.storehaus.cassandra.cql.cascading.{
  CassandraCascadingRowMatcher,
  CassandraCascadingInitializer,
  CassandraSplittingMechanism
}

/**
 * Implementation of generic VersionedCassandraStoreInitializer
 * based on CassandraTupleStore backed by CQLCassandraCollectionStore.
 * Splitting is done based on the native cassandra splitting mechanism.
 */
abstract class VersionedCassandraTupleStoreInitializer[RKT <: Product, CKT <: Product, Value, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
  identifier: String = DEFAULT_VERSIONSTORE_IDENTIFIER,
  versionsToKeep: Int = DEFAULT_VERSIONS_TO_KEEP,
  paramToPreventWritingDownTypes: (RKT, CKT),
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  readConsistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  writeConsistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  readPoolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  writePoolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE,
  metaStoreUnderlying: Option[MetaStoreUnderlyingT] = None)(
    implicit ev1: HListerAux[RKT, RK],
    ev2: HListerAux[CKT, CK],
    ev3: Tupler.Aux[RK, RKT],
    ev4: Tupler.Aux[CK, CKT],
    evrow: Mapped.Aux[RK, CassandraPrimitive, RS],
    evcol: Mapped.Aux[CK, CassandraPrimitive, CS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result[RK, RS],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result[CK, CS],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS],
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
    rsUTC: *->*[CassandraPrimitive]#λ[RS],
    csUTC: *->*[CassandraPrimitive]#λ[CS],
    map0: Mapper.Aux[AbstractCQLCassandraCompositeStore.cassandraSerializerCreation.type, RK, RS],
    map1: Mapper.Aux[AbstractCQLCassandraCompositeStore.cassandraSerializerCreation.type, CK, CS],
    mrk: Mapper.Aux[AbstractCQLCassandraCompositeStore.keyStringMapping.type, RS, MRKResult],
    mck: Mapper.Aux[AbstractCQLCassandraCompositeStore.keyStringMapping.type, CS, MCKResult],
    tork: ToList[MRKResult, String],
    tock: ToList[MCKResult, String],
    ev5: CassandraPrimitive[Value],
    mergeSemigroup: Semigroup[Set[Value]])
  extends VersionedCassandraStoreInitializer[(RKT, CKT), Set[Value]](identifier = identifier, versionsToKeep = versionsToKeep, metaStoreUnderlying = metaStoreUnderlying)
  with CassandraCascadingInitializer[(RKT, CKT), Set[Value]]
  with CassandraTupleStoreConfig[RKT, CKT, Value] {

  // Summary of types used
  type TupleStoreKeyT = (RKT, CKT);
  type TupleStoreValT = Set[Value];
  type InitT = VersionedCassandraTupleStoreInitializer[RKT, CKT, Value, RK, CK, RS, CS, MRKResult, MCKResult];
  type StoreT = CassandraTupleStore[RKT, CKT, Set[Value], RK, CK, RS, CS];
  type SplitterT = CassandraSplittingMechanism[TupleStoreKeyT, TupleStoreValT, InitT];

  @transient private val logger = LoggerFactory.getLogger(classOf[InitT]);

  // Look up serializers
  def rowkeySerializers = {
    getSerializerHListByExample(ev1(paramToPreventWritingDownTypes._1));
  }
  def colkeySerializers = {
    getSerializerHListByExample(ev2(paramToPreventWritingDownTypes._2));
  }

  // We use the native cassandra splitting mechanism (make sure to call this before planning)
  def registerSplittableStoreSplittingMechanism(jobConf: JobConf) = {
    StorehausInputFormat.
      setSplittingClass[TupleStoreKeyT, TupleStoreValT, InitT, SplitterT](
        jobConf, classOf[SplitterT]);
  }

  // CF generator implementation
  override def createColumnFamily(cf: StoreColumnFamily): Unit = {
    CQLCassandraCollectionStore.createColumnFamily(
      cf,
      rowkeySerializers,
      rowkeyColumnNames,
      colkeySerializers,
      colkeyColumnNames,
      ev5,
      Set[Value](),
      valueColumnName);
  }

  // Store factory implementation
  override def createReadableStore(cf: StoreColumnFamily): Store[TupleStoreKeyT, TupleStoreValT] = {
    createTupleStore(cf, readConsistency, readPoolSize);
  }

  // Store factory implementation
  override def createWritableStore(cf: StoreColumnFamily): Store[TupleStoreKeyT, TupleStoreValT] = {
    createTupleStore(cf, writeConsistency, writePoolSize);
  }

  private def createTupleStore(
    cf: StoreColumnFamily,
    consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
    poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE) = {
    new StoreT(
      new CQLCassandraCollectionStore(
        cf,
        rowkeySerializers,
        rowkeyColumnNames,
        colkeySerializers,
        colkeyColumnNames,
        valueColumnName,
        consistency,
        poolSize,
        BatchStatement.Type.UNLOGGED)(
        mergeSemigroup,
        CQLCassandraConfiguration.DEFAULT_SYNC), paramToPreventWritingDownTypes);
  }

  def getCascadingRowMatcher: CassandraCascadingRowMatcher[(RKT, CKT), Set[Value]] = {
    createTupleStore(getCf(-1), readConsistency, readPoolSize);
  }

  def getColumnFamilyName(version: Option[Long]): String = {
    version match {
      case Some(ver) => getCf(ver).getName;
      case None => throw new RuntimeException("Error retrieving CF name with None version");
    }
  }

  def getKeyspaceName: String = { getStoreSession.getKeyspacename }

  def valueSerializer: CassandraPrimitive[Value] = { ev5 }

}
