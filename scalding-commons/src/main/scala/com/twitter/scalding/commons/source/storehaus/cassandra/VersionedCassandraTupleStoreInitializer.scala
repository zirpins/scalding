package com.twitter.scalding.commons.source.storehaus.cassandra

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.mapred.JobConf
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.querybuilder.Clause
import com.twitter.algebird.Semigroup
import com.twitter.storehaus.Store
import com.twitter.storehaus.cascading.StorehausInputFormat
import com.twitter.storehaus.cascading.split.SplittableStore
import com.twitter.storehaus.cascading.split.StorehausSplittingMechanism
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraCompositeStore
import AbstractCQLCassandraCompositeStore._
import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration._
import com.twitter.storehaus.cassandra.cql.CassandraTupleStore
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingInitializer
import com.twitter.storehaus.cassandra.cql.cascading.CassandraSplittingMechanism
import com.websudos.phantom.CassandraPrimitive
import org.slf4j.LoggerFactory
import shapeless._
import shapeless.Tuples._
import shapeless.TypeOperators._
import shapeless.UnaryTCConstraint._
import com.twitter.storehaus.cassandra.cql.cascading.CassandraCascadingRowMatcher

/**
 * Implementation of generic VersionedCassandraStoreInitializer
 * based on CassandraTupleStore backed by CQLCassandraCollectionStore.
 * Splitting is done based on the native cassandra splitting mechanism.
 */
abstract class VersionedCassandraTupleStoreInitializer[RKT <: Product, CKT <: Product, Value, RK <: HList, CK <: HList, RS <: HList, CS <: HList, MRKResult <: HList, MCKResult <: HList](
  paramToPreventWritingDownTypes: (RKT, CKT),
  valueColumnName: String = CQLCassandraConfiguration.DEFAULT_VALUE_COLUMN_NAME,
  consistency: ConsistencyLevel = CQLCassandraConfiguration.DEFAULT_CONSISTENCY_LEVEL,
  poolSize: Int = CQLCassandraConfiguration.DEFAULT_FUTURE_POOL_SIZE)(
    implicit ev1: HListerAux[RKT, RK],
    ev2: HListerAux[CKT, CK],
    ev3: TuplerAux[RK, RKT],
    ev4: TuplerAux[CK, CKT],
    evrow: MappedAux[RK, CassandraPrimitive, RS],
    evcol: MappedAux[CK, CassandraPrimitive, CS],
    rowmap: AbstractCQLCassandraCompositeStore.Row2Result[RK, RS],
    colmap: AbstractCQLCassandraCompositeStore.Row2Result[CK, CS],
    a2cRow: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], RK, RS],
    a2cCol: AbstractCQLCassandraCompositeStore.Append2Composite[ArrayBuffer[Clause], CK, CS],
    rsUTC: *->*[CassandraPrimitive]#λ[RS],
    csUTC: *->*[CassandraPrimitive]#λ[CS],
    map0: MapperAux[AbstractCQLCassandraCompositeStore.cassandraSerializerCreation.type, RK, RS],
    map1: MapperAux[AbstractCQLCassandraCompositeStore.cassandraSerializerCreation.type, CK, CS],
    mrk: MapperAux[AbstractCQLCassandraCompositeStore.keyStringMapping.type, RS, MRKResult],
    mck: MapperAux[AbstractCQLCassandraCompositeStore.keyStringMapping.type, CS, MCKResult],
    tork: ToList[MRKResult, String],
    tock: ToList[MCKResult, String],
    ev5: CassandraPrimitive[Value],
    mergeSemigroup: Semigroup[Set[Value]])
  extends VersionedCassandraStoreInitializer[(RKT, CKT), Set[Value]]
  with CassandraCascadingInitializer[(RKT, CKT), Set[Value]]
  with CassandraTupleStoreConfig[RKT, CKT, Value] {

  // Summary of types used
  type TupleStoreKeyT = (RKT, CKT)
  type TupleStoreValT = Set[Value]
  type InitT = VersionedCassandraTupleStoreInitializer[RKT, CKT, Value, RK, CK, RS, CS, MRKResult, MCKResult]
  type StoreT = CassandraTupleStore[RKT, CKT, Set[Value], RK, CK, RS, CS]
  type SplitterT = CassandraSplittingMechanism[TupleStoreKeyT, TupleStoreValT, InitT]

  @transient private val logger = LoggerFactory.getLogger(classOf[InitT])

  // Look up serializers
  def rowkeySerializers = getSerializerHListByExample(
    paramToPreventWritingDownTypes._1.hlisted)
  def colkeySerializers = getSerializerHListByExample(
    paramToPreventWritingDownTypes._2.hlisted)

  // We use the native cassandra splitting mechanism (make sure to call this before planning)
  def registerSplittableStoreSplittingMechanism(jobConf: JobConf) =
    StorehausInputFormat.
      setSplittingClass[TupleStoreKeyT, TupleStoreValT, InitT, SplitterT](
        jobConf, classOf[SplitterT])

  // CF generator implementation
  override def createColumnFamily(cf: StoreColumnFamily): Unit =
    CQLCassandraCollectionStore.createColumnFamily(
      cf,
      rowkeySerializers,
      rowkeyColumnNames,
      colkeySerializers,
      colkeyColumnNames,
      ev5,
      Set[Value](),
      valueColumnName)

  // Store factory implementation
  override def createStore(cf: StoreColumnFamily): Store[TupleStoreKeyT, TupleStoreValT] =
    createTupleStore(cf)

  private def createTupleStore(cf: StoreColumnFamily) =
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
        CQLCassandraConfiguration.DEFAULT_SYNC), paramToPreventWritingDownTypes)

  def getCascadingRowMatcher: CassandraCascadingRowMatcher[(RKT, CKT), Set[Value]] =
    createTupleStore(getCf(-1))

  def getColumnFamilyName(version: Option[Long]): String =
    version match {
      case Some(ver) => getCf(ver).getName
      case None => throw new RuntimeException("Error retrieving CF name with None version")
    }

  def getKeyspaceName: String = getStoreSession.getKeyspacename

  def valueSerializer: CassandraPrimitive[Value] = ev5

}
