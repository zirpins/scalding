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

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import org.apache.hadoop.conf.Configuration
import com.twitter.scalding._

import com.twitter.storehaus.cascading.StorehausCascadingInitializer
import com.twitter.storehaus.cascading.StorehausTap
import com.twitter.storehaus.cascading.versioned.VersionedStorehausCascadingInitializer
import com.twitter.storehaus.cascading.versioned.VersionedStorehausTap

object StorehausSourcesSpec {
  implicit val mode = new com.twitter.scalding.Test(_ => None)
}

import StorehausSourcesSpec.mode

class StorehausMappableSpec extends Specification with Mockito {
  val init = mock[StorehausCascadingInitializer[String, String]]
  "A StorehausMappable" should {
    "Provide StorehausTaps" in {
      val mappable = new StorehausMappable(init)
      mappable.validateTaps(mode)
    }
  }
}

class StorehausVersionedMappableSpec extends Specification with Mockito {
  val versionedInit = mock[VersionedStorehausCascadingInitializer[String, String]]
  "A StorehausVersionedMappable" should {
    "Provide VersionedStorehausTaps" in {
      val mappable =
        new StorehausVersionedMappable[String, String, VersionedStorehausCascadingInitializer[String, String]](
          versionedInit, 1L)
      mappable.validateTaps(mode)
    }
  }
}
