// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.mill

import java.time.Duration
import kotlin.properties.Delegates
import picocli.CommandLine

class MillFlags {

  @set:CommandLine.Option(
    names = ["--duchy-name"],
    description = ["The name of the duchy that owns this mill."],
    required = true
  )
  var nameOfDuchy by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "PT3S",
    description = ["How long to allow for the gRPC channel to shutdown."],
    required = true
  )
  var channelShutdownTimeout by Delegates.notNull<Duration>()
    private set

  // TODO: figure out a better way to config the duchies' (name, address/port) mapping
  @set:CommandLine.Option(
    names = ["--other-duchy-name-one"],
    description = ["The recognized name of Duchy one"],
    required = true,
    defaultValue = "duchy-name-foo"
  )
  var otherDuchyNameOne by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--other-duchy-computation-control-service-one"],
    description = ["Address and port of the ComputationControlService in Duchy one"],
    required = true,
    defaultValue = "localhost:8081"
  )
  var otherDuchyComputationControlServiceOne by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--other-duchy-public-elgamal-key-one"],
    description = ["The public ElGamal key (132 byte HEX string) in Duchy one"],
    required = true
  )
  var otherDuchyPublicElGamalKeyOne by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--other-duchy-name-two"],
    description = ["The recognized name of Duchy two"],
    required = true,
    defaultValue = "duchy-name-two"
  )
  var otherDuchyNameTwo by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--other-duchy-computation-control-service-two"],
    description = ["Address and port of the ComputationControlService in Duchy two"],
    required = true,
    defaultValue = "localhost:8082"
  )
  var otherDuchyComputationControlServiceTwo by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--other-duchy-public-elgamal-key-two"],
    description = ["The public ElGamal key (132 byte HEX string) in Duchy two"],
    required = true
  )
  var otherDuchyPublicElGamalKeyTwo by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "PT1S",
    description = ["How long to sleep before polling the computation queue again if it is empty."],
    required = true
  )
  var pollingInterval by Delegates.notNull<Duration>()
    private set

  // TODO: switch to use something like keystore to store the keys.
  //  The flag is purely for the ease of initial development.
  @set:CommandLine.Option(
    names = ["--duchy-local-elgamal-key"],
    description = ["This Duchy's local ElGamal key (196 byte HEX string)."],
    required = true
  )
  var duchyLocalElGamalKey by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--combined-public-elgamal-key"],
    description = ["The combined public ElGamal key (132 byte HEX string)"],
    required = true
  )
  var combinedPublicElGamalKey by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--elliptic-curve-id"],
    description = ["The id of the elliptic curve to work on in all crypto operations."],
    required = true
  )
  var ellipticCurveId by Delegates.notNull<Long>()
    private set

  @set:CommandLine.Option(
    names = ["--computation-storage-service-target"],
    required = true
  )
  var computationStorageServiceTarget: String by Delegates.notNull()
    private set

  @set:CommandLine.Option(
    names = ["--mill-id"],
    description = ["The Identifier of the Mill."],
    required = true,
    defaultValue = "mill-0000"
  )
  var millId by Delegates.notNull<String>()
    private set
}
