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

import com.twitter.scalding.AccessMode
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConverter
import com.twitter.storehaus.cascading.StorehausCascadingInitializer
import com.twitter.storehaus.cascading.StorehausTap
import com.twitter.storehaus.cascading.versioned._
import cascading.tap.Tap
import com.twitter.scalding.Mappable

/**
 * Generic typed & mappable source/sink for any storehaus store.
 * - can be sent over the wire and thus can be used in mappers/reducers
 */
case class StorehausMappable[K, V](
  @transient storehausInit: StorehausCascadingInitializer[K, V])(
    implicit conv: TupleConverter[(K, V)]) extends Source with Mappable[(K, V)] {

  override def converter[U >: (K, V)] = TupleConverter.asSuperConverter[(K, V), U](conv)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    new StorehausTap(storehausInit)
}

/**
 * Generic typed & mappable source/sink for any storehaus versioned store.
 * - can be sent over the wire and thus can be used in mappers/reducers
 * - multiple versions of the data can be referenced
 */
case class StorehausVersionedMappable[K, V, Q <: VersionedStorehausCascadingInitializer[K, V]](
  @transient storehausInit: Q, version: Long)(
    implicit conv: TupleConverter[(K, V)]) extends Source with Mappable[(K, V)] {

  override def converter[U >: (K, V)] = TupleConverter.asSuperConverter[(K, V), U](conv)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    new VersionedStorehausTap(storehausInit, version)
  }
}
