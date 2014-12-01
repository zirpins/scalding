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

package com.twitter.scalding.commons.source.storehaus
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.Store

package object cassandra {

  // Default number of versions to keep in version store   
  val DEFAULT_VERSIONS_TO_KEEP = 3;

  // default name suffix for single versioned stores
  val DEFAULT_VERSIONSTORE_IDENTIFIER = "single";

  // Minimum type of the internal versionedCassandraStore meta store
  type MetaStoreT = Store[Long, Boolean] with IterableStore[Long, Boolean];
}
