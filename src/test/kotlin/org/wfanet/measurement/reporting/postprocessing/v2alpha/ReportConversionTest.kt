// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.reporting.postprocessing.v2alpha.MeasurementDetailKt.measurementResult

@RunWith(JUnit4::class)
class ReportConversionTest {
  @Test
  fun `report without custom measurement policy is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_with_custom_policy.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)
    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "union"
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 92459
          standardDeviation = 145777.467021918
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/adacfb57a-fe7b-44b5-9c29-022be610a407"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 18000
          standardDeviation = 185589.5021572231
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a400e54b3-95d1-4056-b92f-f978615a05c3"
        }
        measurementResults += measurementResult {
          reach = 92700
          standardDeviation = 191025.0129033726
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aebc1632a-3676-4fea-b22f-78486f0c48d7"
        }
        measurementResults += measurementResult {
          reach = 163700
          standardDeviation = 196286.4317309566
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a61e13352-b0ff-41eb-879b-dfa7a458d232"
        }
        measurementResults += measurementResult {
          reach = 19100
          standardDeviation = 185668.79847492787
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aa0605786-63a4-4fe1-bbc1-42717bf17ff6"
        }
        measurementResults += measurementResult {
          reach = 127200
          standardDeviation = 193570.0395894004
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/accc56b11-d361-4291-a00b-cef502b50d74"
        }
        measurementResults += measurementResult {
          reach = 224400
          standardDeviation = 200858.04694133752
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/affcf4c2b-d2ec-4083-9db8-8659c6bd5c67"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6932cc4a-d367-43b8-be1c-9b8d7f9e4c93"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ae345574e-76ab-4da6-8b86-8e232453f413"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a1cfe162d-cfac-443e-b71f-c75cf569200c"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a7645a53f-960f-44d7-a13e-8e388ed53f6b"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a063332fa-e5d5-4266-8187-c8c41912fff0"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reach = 40700
          standardDeviation = 137723.42891152142
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/abf7c4421-93e8-469b-99e0-788deb61007d"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/adfa803dc-c829-4ab6-ad3d-7c053c6f50db"
        }
        measurementResults += measurementResult {
          reach = 160800
          standardDeviation = 137766.58800817904
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a7c12b2d1-3fc3-49cd-9da2-565803e4ae73"
        }
        measurementResults += measurementResult {
          reach = 90100
          standardDeviation = 137741.18291657476
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad6800ae9-c0b5-4a34-93aa-2d5fcf6a516d"
        }
        measurementResults += measurementResult {
          reach = 2600
          standardDeviation = 137709.73448185038
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aef5d2c02-d9c7-4e61-82da-e29036941dc2"
        }
        measurementResults += measurementResult {
          reach = 151200
          standardDeviation = 137763.13865252156
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aa45de761-fbb6-4a33-aba5-ff91c9aef4a4"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad3e32ab4-ee58-4a56-bebe-8b6202f4d9f0"
        }
        measurementResults += measurementResult {
          reach = 117800
          standardDeviation = 137751.137096284
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad9b9c1af-b833-4ea8-862a-8f359a67d520"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a07730d66-7036-491b-a708-a79f571f86c3"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a946c76ee-1dfd-415a-b602-bd9fe9d67838"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a410a1c14-1214-449a-b08e-2df1b292dc4a"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/afbdcb99e-a5fa-4edb-b637-6f57a3be9541"
        }
        measurementResults += measurementResult {
          reach = 209100
          standardDeviation = 137783.94126865183
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4ab4336d-cb52-4d1f-b89b-d7b2f4894dfe"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8c6b38c9-308e-4ed7-88cc-c3612bee76ee"
        }
        measurementResults += measurementResult {
          reach = 107500
          standardDeviation = 137747.4358066102
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ae1d4d5ee-b72a-4030-91bc-94a4dd8e5d89"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4f24cd6c-ea6f-42e3-b29e-d07f428624da"
        }
        measurementResults += measurementResult {
          reach = 260000
          standardDeviation = 137802.22629419694
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ae3a4cfd8-bc8b-4b02-87b1-6dce7df82982"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4f704f28-68e9-45c6-8c1a-7d17c5d535f7"
        }
        measurementResults += measurementResult {
          reach = 5000
          standardDeviation = 137710.5971632797
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a235ec530-711c-47dd-a8aa-3dde9c8a710c"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ac9661b70-eba6-4484-9c8a-3d409638fead"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a823e28b3-9dce-4f46-86ed-f7d8fc8949d6"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a58fdcc8b-a4c1-442d-b8b2-656114342868"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab17612dc-03eb-40b6-afe6-e96b75aadf48"
        }
        measurementResults += measurementResult {
          reach = 26900
          standardDeviation = 137718.46888168648
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a2f6c7ba5-7c6d-42db-8def-abeaa9e3d755"
        }
        measurementResults += measurementResult {
          reach = 110900
          standardDeviation = 137748.65760254726
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8e874351-0a58-4efd-892e-e40f1fcdf792"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8ca4d4f4-3385-4d8d-ab47-2e5aa0f79438"
        }
        measurementResults += measurementResult {
          reach = 37000
          standardDeviation = 137722.09906597642
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a924462e5-4b08-4040-8141-6c0b754a9ecf"
        }
        measurementResults += measurementResult {
          reach = 94200
          standardDeviation = 137742.65632427187
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a036b8547-d598-424f-821a-45421bc496aa"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a3cf6e6ea-9089-448f-af38-f145d1cda02e"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aa15f2327-4dff-4f44-b959-174ee843a780"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab4bfe7f1-a8d3-4178-80b4-ed32be304e71"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/af0ae8ced-49ed-4d3a-8755-53dd76bcae8e"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 243539
          standardDeviation = 160194.11206456696
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a5ff4a592-7898-4d33-92e4-9ec3dcfcaf6e"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 121200
          standardDeviation = 193125.85377185547
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4b8c5028-2676-49cd-bf45-9d616f65df7f"
        }
        measurementResults += measurementResult {
          reach = 9200
          standardDeviation = 184955.93290564284
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0a3786cd-292f-444b-b131-c062481c66d7"
        }
        measurementResults += measurementResult {
          reach = 218000
          standardDeviation = 200372.83960256787
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aa1306a4b-8578-415a-b5c4-8b3d108775d9"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4382cd0c-2324-4799-b23a-45df3c361d22"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8e31cafe-8439-4977-858d-285b77a186af"
        }
        measurementResults += measurementResult {
          reach = 95500
          standardDeviation = 191230.74952310062
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6b582365-944c-4083-9134-5d5154cb730e"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ac0db3b2e-c1e0-428d-bed3-f9015381266e"
        }
        measurementResults += measurementResult {
          reach = 338300
          standardDeviation = 209618.90348567747
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a11626fea-9e7d-46e2-a402-e4ae9a99d315"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a03cea144-26db-4f05-b0a2-a75f782698fd"
        }
        measurementResults += measurementResult {
          reach = 182300
          standardDeviation = 197680.09530394306
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a36aee307-f635-4e81-81dc-ad1a33e75562"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "union"
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 59
          standardDeviation = 137388.9419863941
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/abc0e4837-b7e0-4381-93d2-b89a6c8afb9b"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        dataProviders += "edp2"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 158500
          standardDeviation = 195897.94272436583
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0ac32326-51a6-4703-a41f-483ff545aa4e"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a7e3183cf-13cb-4d59-948d-79893608a7e1"
        }
        measurementResults += measurementResult {
          reach = 501400
          standardDeviation = 222577.17028274314
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a4b2693e8-dd5b-4277-96e4-38adbe58bb92"
        }
        measurementResults += measurementResult {
          reach = 17200
          standardDeviation = 185531.84608840532
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6d134c15-6f22-4383-b701-bd7bc5019666"
        }
        measurementResults += measurementResult {
          reach = 25300
          standardDeviation = 186116.15764609864
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aaf3299ac-1e94-4065-b848-ae1de3c2c9cc"
        }
        measurementResults += measurementResult {
          reach = 26200
          standardDeviation = 186181.15565470204
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0d726ff0-4cac-4a5f-b1b5-03af612debf2"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a7445c581-50b8-4fe5-80d0-a550468f1f73"
        }
        measurementResults += measurementResult {
          reach = 341700
          standardDeviation = 209884.07542651
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a17755aa6-bfad-47f4-ab9a-08d8ad169f6a"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6d43010b-9db0-449b-b96c-c2e2d1f8dee6"
        }
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 184302.26284602462
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/acc8eed41-e086-47f2-a5dc-be310a9f5767"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8c978118-8cb1-460e-81b1-c6a904c77fe6"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 103200
          standardDeviation = 137745.89057858166
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a81befaa3-372c-4af9-857d-a9116dbe1bcc"
        }
        measurementResults += measurementResult {
          reach = 9000
          standardDeviation = 137712.03495365262
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a856f8b50-6f52-4382-82f6-39b90fbbd96e"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aec4414d1-61e5-4e79-acdc-179132da5554"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a5ad87b1d-aec1-4b10-b7d1-893a16b347f8"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a1ba90b97-72e3-42eb-9b6d-031b3187e567"
        }
        measurementResults += measurementResult {
          reach = 266200
          standardDeviation = 137804.4533810534
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a6411e2ec-9dfc-43a2-a341-b9b8ae32a737"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad2c3b9e2-c41e-4c80-8e68-d00297015109"
        }
        measurementResults += measurementResult {
          reach = 378100
          standardDeviation = 137844.64252157588
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a282c88f0-5cfa-401c-ab06-9e89146093bc"
        }
        measurementResults += measurementResult {
          reach = 168600
          standardDeviation = 137769.39054605988
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a87ecaedb-34ba-499a-bbba-119dd281350f"
        }
        measurementResults += measurementResult {
          reach = 101700
          standardDeviation = 137745.3515414703
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a02ec75e5-592e-4b55-9a33-b37c8f31955f"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 182759
          standardDeviation = 102064.11288720994
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ab3f072e1-d032-4095-8ac6-4b918bd76560"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a611550b2-4935-4ca2-bbd2-7acefb24fd43"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a1fdb54d1-0cec-4b1b-90c8-e9e2eae03d6b"
        }
        measurementResults += measurementResult {
          reach = 16400
          standardDeviation = 137714.69482626964
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ae46c0e32-1756-40b0-84b4-fe1b78aa2bc4"
        }
        measurementResults += measurementResult {
          reach = 42500
          standardDeviation = 137724.07585876522
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ac2cffd96-33b9-4623-9819-5a8064c32d17"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/afe06ed12-a725-4511-9cda-628707d2b6e4"
        }
        measurementResults += measurementResult {
          reach = 220100
          standardDeviation = 137787.89305141394
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/abdcc16a7-d8c1-40a3-922f-d5b8731255ef"
        }
        measurementResults += measurementResult {
          reach = 171400
          standardDeviation = 137770.39657139347
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a601f869a-9c79-4814-b38d-64729d004907"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a9570e197-d2d7-4214-b551-ac9ed5f2e0e7"
        }
        measurementResults += measurementResult {
          reach = 29500
          standardDeviation = 137719.40339371885
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a3aab690f-3ab5-4e3f-a2fe-d772825027e4"
        }
        measurementResults += measurementResult {
          reach = 113200
          standardDeviation = 137749.4841054185
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a5d97edc1-3224-4462-80d1-7432dbd3716a"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reach = 58679
          standardDeviation = 102028.24324588467
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad1accd8d-d08f-478b-8e59-026a32a7098c"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a678cb4a0-0c33-460f-a0b4-af5f1ed453f2"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a57ce296b-ccaf-4b6d-8daa-9b4abef182b5"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a939d2713-057d-402f-86bf-06b19b123c07"
        }
        measurementResults += measurementResult {
          reach = 141500
          standardDeviation = 137759.6532783536
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aabae6422-c07a-461f-b98a-8ba4c7e93b0a"
        }
        measurementResults += measurementResult {
          reach = 180900
          standardDeviation = 137773.80981688888
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a8e20e2e1-261a-4687-b019-7d4179729311"
        }
        measurementResults += measurementResult {
          reach = 78600
          standardDeviation = 137737.05010356367
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/add37eb62-1d72-4d44-b2df-5c09f5f0573c"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a417fda9c-731f-42fe-9b33-d256c9491e19"
        }
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a210bea30-dce5-4414-8465-04eec62191e9"
        }
        measurementResults += measurementResult {
          reach = 234600
          standardDeviation = 137793.10204643878
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a0502331e-f01b-4034-b026-7d6ccb98e3df"
        }
        measurementResults += measurementResult {
          reach = 11900
          standardDeviation = 137713.07734228627
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/ad74887e2-3bc8-4065-af8c-96434739d527"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 0
          standardDeviation = 230044.8709730047
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a28f7c454-d794-4f00-9cc9-456646578ae9"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 192700
          standardDeviation = 241559.36406894255
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a40719990-249a-4768-96f2-b730cbd8c73e"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "mrc"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        measurementResults += measurementResult {
          reach = 0
          standardDeviation = 230832.33883487116
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/a74457557-66b3-453b-b32b-8b9114684b76"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        measurementResults += measurementResult {
          reach = 0
          standardDeviation = 230060.19095259727
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aad81d6d7-e2a2-4b43-a238-681085395d4a"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "custom"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        measurementResults += measurementResult {
          reach = 35000
          standardDeviation = 232087.23195734533
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/afe55003f-662b-49da-87bc-141083bbf45d"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp1"
        dataProviders += "edp2"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 0
          standardDeviation = 230042.97742153902
          metric = "measurementConsumers/fLhOpt2Z4x8/metrics/aff4b5f41-c344-40d5-b47b-5cdf05b0160f"
        }
      }
    }

    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report without unique reach is successfully converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("sample_report_small.json").toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)
    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 24129432
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/00"
        }
        measurementResults += measurementResult {
          reach = 29152165
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/01"
        }
        measurementResults += measurementResult {
          reach = 31474050
          standardDeviation = 1.0
          metric = "measurementConsumers/TjyUnormbAg/metrics/total/ami/02"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        isCumulative = false
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 1000
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/TjyUnormbAg/metrics/ami/union/00"
        }
      }
    }
    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report with unique reach is successfully converted to report summary proto`() {
    val reportFile =
      TEST_DATA_RUNTIME_DIR.resolve("sample_report_unique_reach_incremental_reach_small.json")
        .toFile()
    val reportAsJson = reportFile.readText()
    val reportSummary = ReportConversion.convertJsontoReportSummaries(reportAsJson)

    val expectedReportSummary = reportSummary {
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 74640
          standardDeviation = 102032.8580350049
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        measurementResults += measurementResult {
          reach = 30000
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp2"
        leftHandSideTargets += "edp2"
        rightHandSideTargets += "edp1"
        rightHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = 2000
          standardDeviation = 262192.75285658165
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp2"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 400
          standardDeviation = 230564.3972774748
          metric = "measurementConsumers/TjyUnormbAg/metrics/a3c0f4fda-c0c9-41f0-bb8e-a88f1354181f"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp1"
        leftHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        rightHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = 300
          standardDeviation = 261177.24408350687
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp1"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp3"
        measurementResults += measurementResult {
          reach = 187439
          standardDeviation = 102065.46555734947
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp3"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp3"
        measurementResults += measurementResult {
          standardDeviation = 137708.79990420336
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp3"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        leftHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = 91199
          standardDeviation = 137993.02905314422
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/all_edps"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        leftHandSideTargets += "edp1"
        leftHandSideTargets += "edp2"
        leftHandSideTargets += "edp3"
        measurementResults += measurementResult {
          reach = 48300
          standardDeviation = 184559.25807765796
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/all_edps"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "union"
        dataProviders += "edp1"
        measurementResults += measurementResult {
          standardDeviation = 102011.27564649425
          metric = "measurementConsumers/TjyUnormbAg/metrics/union/single_edp_edp1"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "cumulative"
        isCumulative = true
        dataProviders += "edp1"
        measurementResults += measurementResult {
          reach = 189700
          standardDeviation = 137776.9714846423
          metric = "measurementConsumers/TjyUnormbAg/metrics/cumulative/single_edp_edp1"
        }
      }
      measurementDetails += measurementDetail {
        measurementPolicy = "ami"
        setOperation = "difference"
        dataProviders += "edp2"
        dataProviders += "edp1"
        dataProviders += "edp3"
        uniqueReachTarget = "edp3"
        leftHandSideTargets += "edp3"
        rightHandSideTargets += "edp1"
        rightHandSideTargets += "edp2"
        measurementResults += measurementResult {
          reach = 100
          standardDeviation = 261663.2405567259
          metric = "measurementConsumers/TjyUnormbAg/metrics/difference/unique_reach_edp3"
        }
      }
    }
    assertThat(reportSummary).hasSize(1)
    assertThat(reportSummary[0]).isEqualTo(expectedReportSummary)
  }

  @Test
  fun `report with unsuccessful state fails to be converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("report_with_unspecified_state.json").toFile()
    val reportAsJson = reportFile.readText()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(reportAsJson)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
  }

  @Test
  fun `report with failed measurement fails to be converted to report summary proto`() {
    val reportFile = TEST_DATA_RUNTIME_DIR.resolve("report_with_failed_measurement.json").toFile()
    val reportAsJson = reportFile.readText()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReportConversion.convertJsontoReportSummaries(reportAsJson)
      }

    assertThat(exception).hasMessageThat().contains("not supported")
  }

  companion object {
    private val TEST_DATA_RUNTIME_DIR: Path =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "test",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "reporting",
          "postprocessing",
          "v2alpha",
        )
      )!!
  }
}
