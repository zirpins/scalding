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

import com.twitter.storehaus.Store

/**
 * Version store extension offering some management functionality
 */
trait ManagedVersionedStore {

  /**
   * return latest version within the version store
   */
  def lastVersion(): Long;

  /**
   * return latest version within the version store
   */
  def versions(): Iterable[Long];

  /**
   * return latest version in store before
   */
  def lastVersionBefore(version: Long): Option[Long];

  /**
   * Shutdown the versioned store initializer
   */
  def close(): Unit;
}
