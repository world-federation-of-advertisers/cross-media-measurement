# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from google.protobuf import text_format

from wfa.measurement.internal.reporting.postprocessing import report_post_processor_result_pb2

from noiseninja.noised_measurements import Measurement, MeasurementSet

from src.main.python.wfa.measurement.reporting.postprocessing.report.report import (
    MetricReport,
    Report,
)
from wfa.measurement.internal.reporting.postprocessing import report_summary_v2_pb2

StatusCode = report_post_processor_result_pb2.ReportPostProcessorStatus.StatusCode

NOISE_CORRECTION_TOLERANCE = 0.1

from tools.post_process_report_summary_v2 import ReportSummaryV2Processor


class TestPostProcessReportSummaryV2(unittest.TestCase):

    def test_report_summary_with_a_single_reach_is_processed_correctly(self):
        report_summary_textproto = """
          cmms_measurement_consumer_id: "NsQ4CS3K1to"
          external_report_result_id: 1896307399873472987
          groupings {
            path: "person.social_grade_group"
            value { enum_value: "C2_D_E" }
          }
          event_filters {
            terms {
              path: "person.age_group"
              value { enum_value: "YEARS_18_TO_34" }
            }
          }
          population: 5733810
          report_summary_set_results {
            external_reporting_set_result_id: 7237497766817761156
            impression_filter: "custom"
            set_operation: "union"
            data_providers: "abc123"
            metric_frequency_spec { weekly: MONDAY }
            non_cumulative_results {
              key {
                non_cumulative_start { year: 2021 month: 3 day: 14 }
                end { year: 2021 month: 3 day: 15 }
              }
              reach {
                value: 116000
                standard_deviation: 137750.79990420336
                metric: "reach_7237497766817761156_non_cumulative_2021_03_15"
              }
            }
          }
        """

        report_summary = text_format.Parse(
            report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        result = ReportSummaryV2Processor(report_summary, []).process()

        self.assertEqual(
            result.status.status_code,
            StatusCode.SOLUTION_FOUND_WITH_HIGHS,
        )
        self.assertEqual(
            result.updated_measurements["reach_7237497766817761156_non_cumulative_2021_03_15"],
            116000,
        )

    def test_report_summary_v2_is_parsed_correctly(self):
        report_summary = get_report_summary_v2(
            'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto'
        )
        reportSummaryProcessor = ReportSummaryV2Processor(report_summary, [])

        reportSummaryProcessor._process_union_results()

        edp1 = frozenset({'EDP_ONE'})
        edp2 = frozenset({'EDP_TWO'})
        edp3 = frozenset({'EDP_THREE'})
        edp12 = frozenset({'EDP_ONE', 'EDP_TWO'})
        edp123 = frozenset({'EDP_ONE', 'EDP_TWO', 'EDP_THREE'})

        expected_weekly_cumulative_reaches = {
            'ami': {
                edp1: [
                    Measurement(9992500, 10000, 'm_001'),
                    Measurement(11998422, 10000, 'm_002'),
                ],
                edp2: [
                    Measurement(5000000, 0, 'm_012'),
                    Measurement(6000000, 0, 'm_013'),
                ],
                edp3: [
                    Measurement(800000, 0, 'm_023'),
                    Measurement(1000000, 0, 'm_024'),
                ],
                edp123: [
                    Measurement(15830545, 10000, 'm_036'),
                    Measurement(19010669, 10000, 'm_037'),
                ],
            },
            'mrc': {
                edp1: [
                    Measurement(9501618, 10000, 'm_047'),
                    Measurement(11389309, 10000, 'm_048'),
                ],
                edp2: [
                    Measurement(4750000, 0, 'm_058'),
                    Measurement(5700000, 0, 'm_059'),
                ],
                edp3: [
                    Measurement(760000, 0, 'm_069'),
                    Measurement(950000, 0, 'm_070'),
                ],
                edp12: [
                    Measurement(13427250, 10000, 'm_080'),
                    Measurement(15920317, 10000, 'm_081'),
                ],
            },
            'custom': {
                edp1: [
                    Measurement(9984642, 10000, 'm_091'),
                    Measurement(12020226, 10000, 'm_092'),
                ],
                edp2: [
                    Measurement(5000000, 0, 'm_102'),
                    Measurement(6000000, 0, 'm_103'),
                ],
                edp3: [
                    Measurement(800000, 0, 'm_113'),
                    Measurement(1000000, 0, 'm_114'),
                ],
                edp123: [
                    Measurement(15799013, 10000, 'm_124'),
                    Measurement(19015392, 10000, 'm_125'),
                ],
            },
        }

        expected_whole_campaign_measurements = {
            'ami': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(11978894, 10000, 'm_003'),
                        k_reach={
                            1: Measurement(6182655, 10000, 'm_004-bin-1'),
                            2: Measurement(3091328, 10000, 'm_004-bin-2'),
                            3: Measurement(1545664, 10000, 'm_004-bin-3'),
                            4: Measurement(772832, 10000, 'm_004-bin-4'),
                            5: Measurement(386415, 10000, 'm_004-bin-5'),
                        },
                        impression=Measurement(22870892, 10000, 'm_005'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(6000000, 0, 'm_014'),
                        k_reach={
                            1: Measurement(3096774, 0, 'm_015-bin-1'),
                            2: Measurement(1548387, 0, 'm_015-bin-2'),
                            3: Measurement(774194, 0, 'm_015-bin-3'),
                            4: Measurement(387097, 0, 'm_015-bin-4'),
                            5: Measurement(193548, 0, 'm_015-bin-5'),
                        },
                        impression=Measurement(11216125, 0, 'm_016'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(1000000, 0, 'm_025'),
                        k_reach={
                            1: Measurement(516129, 0, 'm_026-bin-1'),
                            2: Measurement(258065, 0, 'm_026-bin-2'),
                            3: Measurement(129033, 0, 'm_026-bin-3'),
                            4: Measurement(64517, 0, 'm_026-bin-4'),
                            5: Measurement(32256, 0, 'm_026-bin-5'),
                        },
                        impression=Measurement(1844136, 0, 'm_027'),
                    ),
                edp12:
                    MeasurementSet(
                        reach=Measurement(16686873, 10000, 'm_034'),
                        impression=Measurement(34113188, 10000, 'm_035'),
                    ),
                edp123:
                    MeasurementSet(
                        reach=Measurement(19021738, 10000, 'm_038'),
                        k_reach={
                            1: Measurement(9817671, 10000, 'm_039-bin-1'),
                            2: Measurement(4908836, 10000, 'm_039-bin-2'),
                            3: Measurement(2454418, 10000, 'm_039-bin-3'),
                            4: Measurement(1227209, 10000, 'm_039-bin-4'),
                            5: Measurement(613604, 10000, 'm_039-bin-5'),
                        },
                        impression=Measurement(35926461, 10000, 'm_040'),
                    ),
            },
            'mrc': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(11382243, 10000, 'm_049'),
                        k_reach={
                            1: Measurement(5874706, 10000, 'm_050-bin-1'),
                            2: Measurement(2937353, 10000, 'm_050-bin-2'),
                            3: Measurement(1468677, 10000, 'm_050-bin-3'),
                            4: Measurement(734339, 10000, 'm_050-bin-4'),
                            5: Measurement(367168, 10000, 'm_050-bin-5'),
                        },
                        impression=Measurement(21696322, 10000, 'm_051'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(5700000, 0, 'm_060'),
                        k_reach={
                            1: Measurement(2941935, 0, 'm_061-bin-1'),
                            2: Measurement(1470968, 0, 'm_061-bin-2'),
                            3: Measurement(735484, 0, 'm_061-bin-3'),
                            4: Measurement(367742, 0, 'm_061-bin-4'),
                            5: Measurement(183871, 0, 'm_061-bin-5'),
                        },
                        impression=Measurement(10645760, 0, 'm_062'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(950000, 0, 'm_071'),
                        k_reach={
                            1: Measurement(490323, 0, 'm_072-bin-1'),
                            2: Measurement(245162, 0, 'm_072-bin-2'),
                            3: Measurement(122581, 0, 'm_072-bin-3'),
                            4: Measurement(61291, 0, 'm_072-bin-4'),
                            5: Measurement(30643, 0, 'm_072-bin-5'),
                        },
                        impression=Measurement(1751669, 0, 'm_073'),
                    ),
                edp12:
                    MeasurementSet(
                        reach=Measurement(15908881, 10000, 'm_082'),
                        k_reach={
                            1: Measurement(8211035, 10000, 'm_083-bin-1'),
                            2: Measurement(4105518, 10000, 'm_083-bin-2'),
                            3: Measurement(2052759, 10000, 'm_083-bin-3'),
                            4: Measurement(1026380, 10000, 'm_083-bin-4'),
                            5: Measurement(513189, 10000, 'm_083-bin-5'),
                        },
                        impression=Measurement(32337826, 10000, 'm_084'),
                    ),
            },
            'custom': {
                edp1:
                    MeasurementSet(
                        reach=Measurement(12017026, 10000, 'm_093'),
                        k_reach={
                            1: Measurement(6202336, 10000, 'm_094-bin-1'),
                            2: Measurement(3101168, 10000, 'm_094-bin-2'),
                            3: Measurement(1550584, 10000, 'm_094-bin-3'),
                            4: Measurement(775292, 10000, 'm_094-bin-4'),
                            5: Measurement(387646, 10000, 'm_094-bin-5'),
                        },
                        impression=Measurement(22871159, 10000, 'm_095'),
                    ),
                edp2:
                    MeasurementSet(
                        reach=Measurement(6000000, 0, 'm_104'),
                        k_reach={
                            1: Measurement(3096774, 0, 'm_105-bin-1'),
                            2: Measurement(1548387, 0, 'm_105-bin-2'),
                            3: Measurement(774194, 0, 'm_105-bin-3'),
                            4: Measurement(387097, 0, 'm_105-bin-4'),
                            5: Measurement(193548, 0, 'm_105-bin-5'),
                        },
                        impression=Measurement(11216125, 0, 'm_106'),
                    ),
                edp3:
                    MeasurementSet(
                        reach=Measurement(1000000, 0, 'm_115'),
                        k_reach={
                            1: Measurement(516129, 0, 'm_116-bin-1'),
                            2: Measurement(258065, 0, 'm_116-bin-2'),
                            3: Measurement(129033, 0, 'm_116-bin-3'),
                            4: Measurement(64517, 0, 'm_116-bin-4'),
                            5: Measurement(32256, 0, 'm_116-bin-5'),
                        },
                        impression=Measurement(1844136, 0, 'm_117'),
                    ),
                edp123:
                    MeasurementSet(
                        reach=Measurement(19030737, 10000, 'm_126'),
                        k_reach={
                            1: Measurement(9822316, 10000, 'm_127-bin-1'),
                            2: Measurement(4911158, 10000, 'm_127-bin-2'),
                            3: Measurement(2455579, 10000, 'm_127-bin-3'),
                            4: Measurement(1227790, 10000, 'm_127-bin-4'),
                            5: Measurement(613894, 10000, 'm_127-bin-5'),
                        },
                        impression=Measurement(35936915, 10000, 'm_128'),
                    ),
            },
        }

        expected_weekly_non_cumulative_measurements = {
            'ami': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(10008130, 10000, 'm_006'),
                        k_reach={
                            1: Measurement(5165486, 10000, 'm_007-bin-1'),
                            2: Measurement(2582743, 10000, 'm_007-bin-2'),
                            3: Measurement(1291372, 10000, 'm_007-bin-3'),
                            4: Measurement(645686, 10000, 'm_007-bin-4'),
                            5: Measurement(322843, 10000, 'm_007-bin-5'),
                        },
                        impression=Measurement(18379493, 10000, 'm_008'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2452001, 10000, 'm_009'),
                        k_reach={
                            1: Measurement(1265549, 10000, 'm_010-bin-1'),
                            2: Measurement(632775, 10000, 'm_010-bin-2'),
                            3: Measurement(316388, 10000, 'm_010-bin-3'),
                            4: Measurement(158194, 10000, 'm_010-bin-4'),
                            5: Measurement(79095, 10000, 'm_010-bin-5'),
                        },
                        impression=Measurement(4471035, 10000, 'm_011'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(5000000, 0, 'm_017'),
                        k_reach={
                            1: Measurement(2580645, 0, 'm_018-bin-1'),
                            2: Measurement(1290323, 0, 'm_018-bin-2'),
                            3: Measurement(645162, 0, 'm_018-bin-3'),
                            4: Measurement(322581, 0, 'm_018-bin-4'),
                            5: Measurement(161289, 0, 'm_018-bin-5'),
                        },
                        impression=Measurement(9193546, 0, 'm_019'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1100000, 0, 'm_020'),
                        k_reach={
                            1: Measurement(567742, 0, 'm_021-bin-1'),
                            2: Measurement(283871, 0, 'm_021-bin-2'),
                            3: Measurement(141936, 0, 'm_021-bin-3'),
                            4: Measurement(70968, 0, 'm_021-bin-4'),
                            5: Measurement(35483, 0, 'm_021-bin-5'),
                        },
                        impression=Measurement(2022579, 0, 'm_022'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(800000, 0, 'm_028'),
                        k_reach={
                            1: Measurement(412903, 0, 'm_029-bin-1'),
                            2: Measurement(206452, 0, 'm_029-bin-2'),
                            3: Measurement(103226, 0, 'm_029-bin-3'),
                            4: Measurement(51613, 0, 'm_029-bin-4'),
                            5: Measurement(25806, 0, 'm_029-bin-5'),
                        },
                        impression=Measurement(1470967, 0, 'm_030'),
                    ),
                    MeasurementSet(
                        reach=Measurement(202952, 0, 'm_031'),
                        k_reach={
                            1: Measurement(104749, 0, 'm_032-bin-1'),
                            2: Measurement(52375, 0, 'm_032-bin-2'),
                            3: Measurement(26188, 0, 'm_032-bin-3'),
                            4: Measurement(13094, 0, 'm_032-bin-4'),
                            5: Measurement(6546, 0, 'm_032-bin-5'),
                        },
                        impression=Measurement(373169, 0, 'm_033'),
                    ),
                ],
                edp123: [
                    MeasurementSet(
                        reach=Measurement(15829304, 10000, 'm_041'),
                        k_reach={
                            1: Measurement(8169963, 10000, 'm_042-bin-1'),
                            2: Measurement(4084982, 10000, 'm_042-bin-2'),
                            3: Measurement(2042491, 10000, 'm_042-bin-3'),
                            4: Measurement(1021246, 10000, 'm_042-bin-4'),
                            5: Measurement(510622, 10000, 'm_042-bin-5'),
                        },
                        impression=Measurement(29046331, 10000, 'm_043'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3761510, 10000, 'm_044'),
                        k_reach={
                            1: Measurement(1941425, 10000, 'm_045-bin-1'),
                            2: Measurement(970713, 10000, 'm_045-bin-2'),
                            3: Measurement(485357, 10000, 'm_045-bin-3'),
                            4: Measurement(242679, 10000, 'm_045-bin-4'),
                            5: Measurement(121336, 10000, 'm_045-bin-5'),
                        },
                        impression=Measurement(6904573, 10000, 'm_046'),
                    ),
                ],
            },
            'mrc': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(9503446, 10000, 'm_052'),
                        k_reach={
                            1: Measurement(4905004, 10000, 'm_053-bin-1'),
                            2: Measurement(2452502, 10000, 'm_053-bin-2'),
                            3: Measurement(1226251, 10000, 'm_053-bin-3'),
                            4: Measurement(613126, 10000, 'm_053-bin-4'),
                            5: Measurement(306563, 10000, 'm_053-bin-5'),
                        },
                        impression=Measurement(17473517, 10000, 'm_054'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2289252, 10000, 'm_055'),
                        k_reach={
                            1: Measurement(1181549, 10000, 'm_056-bin-1'),
                            2: Measurement(590775, 10000, 'm_056-bin-2'),
                            3: Measurement(295388, 10000, 'm_056-bin-3'),
                            4: Measurement(147694, 10000, 'm_056-bin-4'),
                            5: Measurement(73846, 10000, 'm_056-bin-5'),
                        },
                        impression=Measurement(4236753, 10000, 'm_057'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(4750000, 0, 'm_063'),
                        k_reach={
                            1: Measurement(2451613, 0, 'm_064-bin-1'),
                            2: Measurement(1225807, 0, 'm_064-bin-2'),
                            3: Measurement(612904, 0, 'm_064-bin-3'),
                            4: Measurement(306452, 0, 'm_064-bin-4'),
                            5: Measurement(153224, 0, 'm_064-bin-5'),
                        },
                        impression=Measurement(8733867, 0, 'm_065'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1039801, 0, 'm_066'),
                        k_reach={
                            1: Measurement(536671, 0, 'm_067-bin-1'),
                            2: Measurement(268336, 0, 'm_067-bin-2'),
                            3: Measurement(134168, 0, 'm_067-bin-3'),
                            4: Measurement(67084, 0, 'm_067-bin-4'),
                            5: Measurement(33542, 0, 'm_067-bin-5'),
                        },
                        impression=Measurement(1911893, 0, 'm_068'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(760000, 0, 'm_074'),
                        k_reach={
                            1: Measurement(392258, 0, 'm_075-bin-1'),
                            2: Measurement(196129, 0, 'm_075-bin-2'),
                            3: Measurement(98065, 0, 'm_075-bin-3'),
                            4: Measurement(49033, 0, 'm_075-bin-4'),
                            5: Measurement(24515, 0, 'm_075-bin-5'),
                        },
                        impression=Measurement(1397418, 0, 'm_076'),
                    ),
                    MeasurementSet(
                        reach=Measurement(192662, 0, 'm_077'),
                        k_reach={
                            1: Measurement(99438, 0, 'm_078-bin-1'),
                            2: Measurement(49719, 0, 'm_078-bin-2'),
                            3: Measurement(24860, 0, 'm_078-bin-3'),
                            4: Measurement(12430, 0, 'm_078-bin-4'),
                            5: Measurement(6215, 0, 'm_078-bin-5'),
                        },
                        impression=Measurement(354251, 0, 'm_079'),
                    ),
                ],
                edp12: [
                    MeasurementSet(
                        reach=Measurement(13426464, 10000, 'm_085'),
                        k_reach={
                            1: Measurement(6929788, 10000, 'm_086-bin-1'),
                            2: Measurement(3464894, 10000, 'm_086-bin-2'),
                            3: Measurement(1732447, 10000, 'm_086-bin-3'),
                            4: Measurement(866224, 10000, 'm_086-bin-4'),
                            5: Measurement(433111, 10000, 'm_086-bin-5'),
                        },
                        impression=Measurement(26215389, 10000, 'm_087'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3278136, 10000, 'm_088'),
                        k_reach={
                            1: Measurement(1691941, 10000, 'm_089-bin-1'),
                            2: Measurement(845971, 10000, 'm_089-bin-2'),
                            3: Measurement(422986, 10000, 'm_089-bin-3'),
                            4: Measurement(211493, 10000, 'm_089-bin-4'),
                            5: Measurement(105745, 10000, 'm_089-bin-5'),
                        },
                        impression=Measurement(6135862, 10000, 'm_090'),
                    ),
                ],
            },
            'custom': {
                edp1: [
                    MeasurementSet(
                        reach=Measurement(10000981, 10000, 'm_096'),
                        k_reach={
                            1: Measurement(5161797, 10000, 'm_097-bin-1'),
                            2: Measurement(2580899, 10000, 'm_097-bin-2'),
                            3: Measurement(1290450, 10000, 'm_097-bin-3'),
                            4: Measurement(645225, 10000, 'm_097-bin-4'),
                            5: Measurement(322610, 10000, 'm_097-bin-5'),
                        },
                        impression=Measurement(18382797, 10000, 'm_098'),
                    ),
                    MeasurementSet(
                        reach=Measurement(2441042, 10000, 'm_099'),
                        k_reach={
                            1: Measurement(1259893, 10000, 'm_100-bin-1'),
                            2: Measurement(629947, 10000, 'm_100-bin-2'),
                            3: Measurement(314974, 10000, 'm_100-bin-3'),
                            4: Measurement(157487, 10000, 'm_100-bin-4'),
                            5: Measurement(78741, 10000, 'm_100-bin-5'),
                        },
                        impression=Measurement(4488362, 10000, 'm_101'),
                    ),
                ],
                edp2: [
                    MeasurementSet(
                        reach=Measurement(5000000, 0, 'm_107'),
                        k_reach={
                            1: Measurement(2580645, 0, 'm_108-bin-1'),
                            2: Measurement(1290323, 0, 'm_108-bin-2'),
                            3: Measurement(645162, 0, 'm_108-bin-3'),
                            4: Measurement(322581, 0, 'm_108-bin-4'),
                            5: Measurement(161289, 0, 'm_108-bin-5'),
                        },
                        impression=Measurement(9193546, 0, 'm_109'),
                    ),
                    MeasurementSet(
                        reach=Measurement(1100000, 0, 'm_110'),
                        k_reach={
                            1: Measurement(567742, 0, 'm_111-bin-1'),
                            2: Measurement(283871, 0, 'm_111-bin-2'),
                            3: Measurement(141936, 0, 'm_111-bin-3'),
                            4: Measurement(70968, 0, 'm_111-bin-4'),
                            5: Measurement(35483, 0, 'm_111-bin-5'),
                        },
                        impression=Measurement(2022579, 0, 'm_112'),
                    ),
                ],
                edp3: [
                    MeasurementSet(
                        reach=Measurement(800000, 0, 'm_118'),
                        k_reach={
                            1: Measurement(412903, 0, 'm_119-bin-1'),
                            2: Measurement(206452, 0, 'm_119-bin-2'),
                            3: Measurement(103226, 0, 'm_119-bin-3'),
                            4: Measurement(51613, 0, 'm_119-bin-4'),
                            5: Measurement(25806, 0, 'm_119-bin-5'),
                        },
                        impression=Measurement(1470967, 0, 'm_120'),
                    ),
                    MeasurementSet(
                        reach=Measurement(202952, 0, 'm_121'),
                        k_reach={
                            1: Measurement(104749, 0, 'm_122-bin-1'),
                            2: Measurement(52375, 0, 'm_122-bin-2'),
                            3: Measurement(26188, 0, 'm_122-bin-3'),
                            4: Measurement(13094, 0, 'm_122-bin-4'),
                            5: Measurement(6546, 0, 'm_122-bin-5'),
                        },
                        impression=Measurement(373169, 0, 'm_123'),
                    ),
                ],
                edp123: [
                    MeasurementSet(
                        reach=Measurement(15819974, 10000, 'm_129'),
                        k_reach={
                            1: Measurement(8165148, 10000, 'm_130-bin-1'),
                            2: Measurement(4082574, 10000, 'm_130-bin-2'),
                            3: Measurement(2041287, 10000, 'm_130-bin-3'),
                            4: Measurement(1020644, 10000, 'm_130-bin-4'),
                            5: Measurement(510321, 10000, 'm_130-bin-5'),
                        },
                        impression=Measurement(29052805, 10000, 'm_131'),
                    ),
                    MeasurementSet(
                        reach=Measurement(3751542, 10000, 'm_132'),
                        k_reach={
                            1: Measurement(1936280, 10000, 'm_133-bin-1'),
                            2: Measurement(968140, 10000, 'm_133-bin-2'),
                            3: Measurement(484070, 10000, 'm_133-bin-3'),
                            4: Measurement(242035, 10000, 'm_133-bin-4'),
                            5: Measurement(121017, 10000, 'm_133-bin-5'),
                        },
                        impression=Measurement(6884110, 10000, 'm_134'),
                    ),
                ],
            },
        }

        self.assertEqual(
            reportSummaryProcessor._weekly_cumulative_reaches,
            expected_weekly_cumulative_reaches,
        )
        self.assertEqual(
            reportSummaryProcessor._whole_campaign_measurements,
            expected_whole_campaign_measurements,
        )
        self.assertEqual(
            reportSummaryProcessor._weekly_non_cumulative_measurements,
            expected_weekly_non_cumulative_measurements,
        )

    def test_report_is_built_from_report_summary_v2_correctly(self):
        report_summary = get_report_summary_v2(
            'src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto'
        )
        reportSummaryProcessor = ReportSummaryV2Processor(report_summary, [])

        edp1 = frozenset({'EDP_ONE'})
        edp2 = frozenset({'EDP_TWO'})
        edp3 = frozenset({'EDP_THREE'})
        edp12 = frozenset({'EDP_ONE', 'EDP_TWO'})
        edp123 = frozenset({'EDP_ONE', 'EDP_TWO', 'EDP_THREE'})

        report = reportSummaryProcessor._build_report()

        expected_report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9992500, 10000, 'm_001'),
                                Measurement(11998422, 10000, 'm_002'),
                            ],
                            edp2: [
                                Measurement(5000000, 0, 'm_012'),
                                Measurement(6000000, 0, 'm_013'),
                            ],
                            edp3: [
                                Measurement(800000, 0, 'm_023'),
                                Measurement(1000000, 0, 'm_024'),
                            ],
                            edp123: [
                                Measurement(15830545, 10000, 'm_036'),
                                Measurement(19010669, 10000, 'm_037'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(11978894, 10000, 'm_003'),
                                    k_reach={
                                        1:
                                            Measurement(6182655, 10000,
                                                        'm_004-bin-1'),
                                        2:
                                            Measurement(3091328, 10000,
                                                        'm_004-bin-2'),
                                        3:
                                            Measurement(1545664, 10000,
                                                        'm_004-bin-3'),
                                        4:
                                            Measurement(772832, 10000,
                                                        'm_004-bin-4'),
                                        5:
                                            Measurement(386415, 10000,
                                                        'm_004-bin-5'),
                                    },
                                    impression=Measurement(
                                        22870892, 10000, 'm_005'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(6000000, 0, 'm_014'),
                                    k_reach={
                                        1:
                                            Measurement(3096774, 0,
                                                        'm_015-bin-1'),
                                        2:
                                            Measurement(1548387, 0,
                                                        'm_015-bin-2'),
                                        3:
                                            Measurement(774194, 0,
                                                        'm_015-bin-3'),
                                        4:
                                            Measurement(387097, 0,
                                                        'm_015-bin-4'),
                                        5:
                                            Measurement(193548, 0,
                                                        'm_015-bin-5'),
                                    },
                                    impression=Measurement(
                                        11216125, 0, 'm_016'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(1000000, 0, 'm_025'),
                                    k_reach={
                                        1:
                                            Measurement(516129, 0,
                                                        'm_026-bin-1'),
                                        2:
                                            Measurement(258065, 0,
                                                        'm_026-bin-2'),
                                        3:
                                            Measurement(129033, 0,
                                                        'm_026-bin-3'),
                                        4:
                                            Measurement(64517, 0,
                                                        'm_026-bin-4'),
                                        5:
                                            Measurement(32256, 0,
                                                        'm_026-bin-5'),
                                    },
                                    impression=Measurement(1844136, 0, 'm_027'),
                                ),
                            edp12:
                                MeasurementSet(
                                    reach=Measurement(16686873, 10000, 'm_034'),
                                    impression=Measurement(
                                        34113188, 10000, 'm_035'),
                                ),
                            edp123:
                                MeasurementSet(
                                    reach=Measurement(19021738, 10000, 'm_038'),
                                    k_reach={
                                        1:
                                            Measurement(9817671, 10000,
                                                        'm_039-bin-1'),
                                        2:
                                            Measurement(4908836, 10000,
                                                        'm_039-bin-2'),
                                        3:
                                            Measurement(2454418, 10000,
                                                        'm_039-bin-3'),
                                        4:
                                            Measurement(1227209, 10000,
                                                        'm_039-bin-4'),
                                        5:
                                            Measurement(613604, 10000,
                                                        'm_039-bin-5'),
                                    },
                                    impression=Measurement(
                                        35926461, 10000, 'm_040'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(10008130, 10000, 'm_006'),
                                    k_reach={
                                        1:
                                            Measurement(5165486, 10000,
                                                        'm_007-bin-1'),
                                        2:
                                            Measurement(2582743, 10000,
                                                        'm_007-bin-2'),
                                        3:
                                            Measurement(1291372, 10000,
                                                        'm_007-bin-3'),
                                        4:
                                            Measurement(645686, 10000,
                                                        'm_007-bin-4'),
                                        5:
                                            Measurement(322843, 10000,
                                                        'm_007-bin-5'),
                                    },
                                    impression=Measurement(
                                        18379493, 10000, 'm_008'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2452001, 10000, 'm_009'),
                                    k_reach={
                                        1:
                                            Measurement(1265549, 10000,
                                                        'm_010-bin-1'),
                                        2:
                                            Measurement(632775, 10000,
                                                        'm_010-bin-2'),
                                        3:
                                            Measurement(316388, 10000,
                                                        'm_010-bin-3'),
                                        4:
                                            Measurement(158194, 10000,
                                                        'm_010-bin-4'),
                                        5:
                                            Measurement(79095, 10000,
                                                        'm_010-bin-5'),
                                    },
                                    impression=Measurement(
                                        4471035, 10000, 'm_011'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(5000000, 0, 'm_017'),
                                    k_reach={
                                        1:
                                            Measurement(2580645, 0,
                                                        'm_018-bin-1'),
                                        2:
                                            Measurement(1290323, 0,
                                                        'm_018-bin-2'),
                                        3:
                                            Measurement(645162, 0,
                                                        'm_018-bin-3'),
                                        4:
                                            Measurement(322581, 0,
                                                        'm_018-bin-4'),
                                        5:
                                            Measurement(161289, 0,
                                                        'm_018-bin-5'),
                                    },
                                    impression=Measurement(9193546, 0, 'm_019'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1100000, 0, 'm_020'),
                                    k_reach={
                                        1:
                                            Measurement(567742, 0,
                                                        'm_021-bin-1'),
                                        2:
                                            Measurement(283871, 0,
                                                        'm_021-bin-2'),
                                        3:
                                            Measurement(141936, 0,
                                                        'm_021-bin-3'),
                                        4:
                                            Measurement(70968, 0,
                                                        'm_021-bin-4'),
                                        5:
                                            Measurement(35483, 0,
                                                        'm_021-bin-5'),
                                    },
                                    impression=Measurement(2022579, 0, 'm_022'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(800000, 0, 'm_028'),
                                    k_reach={
                                        1:
                                            Measurement(412903, 0,
                                                        'm_029-bin-1'),
                                        2:
                                            Measurement(206452, 0,
                                                        'm_029-bin-2'),
                                        3:
                                            Measurement(103226, 0,
                                                        'm_029-bin-3'),
                                        4:
                                            Measurement(51613, 0,
                                                        'm_029-bin-4'),
                                        5:
                                            Measurement(25806, 0,
                                                        'm_029-bin-5'),
                                    },
                                    impression=Measurement(1470967, 0, 'm_030'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(202952, 0, 'm_031'),
                                    k_reach={
                                        1:
                                            Measurement(104749, 0,
                                                        'm_032-bin-1'),
                                        2:
                                            Measurement(52375, 0,
                                                        'm_032-bin-2'),
                                        3:
                                            Measurement(26188, 0,
                                                        'm_032-bin-3'),
                                        4:
                                            Measurement(13094, 0,
                                                        'm_032-bin-4'),
                                        5:
                                            Measurement(6546, 0, 'm_032-bin-5'),
                                    },
                                    impression=Measurement(373169, 0, 'm_033'),
                                ),
                            ],
                            edp123: [
                                MeasurementSet(
                                    reach=Measurement(15829304, 10000, 'm_041'),
                                    k_reach={
                                        1:
                                            Measurement(8169963, 10000,
                                                        'm_042-bin-1'),
                                        2:
                                            Measurement(4084982, 10000,
                                                        'm_042-bin-2'),
                                        3:
                                            Measurement(2042491, 10000,
                                                        'm_042-bin-3'),
                                        4:
                                            Measurement(1021246, 10000,
                                                        'm_042-bin-4'),
                                        5:
                                            Measurement(510622, 10000,
                                                        'm_042-bin-5'),
                                    },
                                    impression=Measurement(
                                        29046331, 10000, 'm_043'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3761510, 10000, 'm_044'),
                                    k_reach={
                                        1:
                                            Measurement(1941425, 10000,
                                                        'm_045-bin-1'),
                                        2:
                                            Measurement(970713, 10000,
                                                        'm_045-bin-2'),
                                        3:
                                            Measurement(485357, 10000,
                                                        'm_045-bin-3'),
                                        4:
                                            Measurement(242679, 10000,
                                                        'm_045-bin-4'),
                                        5:
                                            Measurement(121336, 10000,
                                                        'm_045-bin-5'),
                                    },
                                    impression=Measurement(
                                        6904573, 10000, 'm_046'),
                                ),
                            ],
                        }),
                "mrc":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9501618, 10000, 'm_047'),
                                Measurement(11389309, 10000, 'm_048'),
                            ],
                            edp2: [
                                Measurement(4750000, 0, 'm_058'),
                                Measurement(5700000, 0, 'm_059'),
                            ],
                            edp3: [
                                Measurement(760000, 0, 'm_069'),
                                Measurement(950000, 0, 'm_070'),
                            ],
                            edp12: [
                                Measurement(13427250, 10000, 'm_080'),
                                Measurement(15920317, 10000, 'm_081'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(11382243, 10000, 'm_049'),
                                    k_reach={
                                        1:
                                            Measurement(5874706, 10000,
                                                        'm_050-bin-1'),
                                        2:
                                            Measurement(2937353, 10000,
                                                        'm_050-bin-2'),
                                        3:
                                            Measurement(1468677, 10000,
                                                        'm_050-bin-3'),
                                        4:
                                            Measurement(734339, 10000,
                                                        'm_050-bin-4'),
                                        5:
                                            Measurement(367168, 10000,
                                                        'm_050-bin-5'),
                                    },
                                    impression=Measurement(
                                        21696322, 10000, 'm_051'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(5700000, 0, 'm_060'),
                                    k_reach={
                                        1:
                                            Measurement(2941935, 0,
                                                        'm_061-bin-1'),
                                        2:
                                            Measurement(1470968, 0,
                                                        'm_061-bin-2'),
                                        3:
                                            Measurement(735484, 0,
                                                        'm_061-bin-3'),
                                        4:
                                            Measurement(367742, 0,
                                                        'm_061-bin-4'),
                                        5:
                                            Measurement(183871, 0,
                                                        'm_061-bin-5'),
                                    },
                                    impression=Measurement(
                                        10645760, 0, 'm_062'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(950000, 0, 'm_071'),
                                    k_reach={
                                        1:
                                            Measurement(490323, 0,
                                                        'm_072-bin-1'),
                                        2:
                                            Measurement(245162, 0,
                                                        'm_072-bin-2'),
                                        3:
                                            Measurement(122581, 0,
                                                        'm_072-bin-3'),
                                        4:
                                            Measurement(61291, 0,
                                                        'm_072-bin-4'),
                                        5:
                                            Measurement(30643, 0,
                                                        'm_072-bin-5'),
                                    },
                                    impression=Measurement(1751669, 0, 'm_073'),
                                ),
                            edp12:
                                MeasurementSet(
                                    reach=Measurement(15908881, 10000, 'm_082'),
                                    k_reach={
                                        1:
                                            Measurement(8211035, 10000,
                                                        'm_083-bin-1'),
                                        2:
                                            Measurement(4105518, 10000,
                                                        'm_083-bin-2'),
                                        3:
                                            Measurement(2052759, 10000,
                                                        'm_083-bin-3'),
                                        4:
                                            Measurement(1026380, 10000,
                                                        'm_083-bin-4'),
                                        5:
                                            Measurement(513189, 10000,
                                                        'm_083-bin-5'),
                                    },
                                    impression=Measurement(
                                        32337826, 10000, 'm_084'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(9503446, 10000, 'm_052'),
                                    k_reach={
                                        1:
                                            Measurement(4905004, 10000,
                                                        'm_053-bin-1'),
                                        2:
                                            Measurement(2452502, 10000,
                                                        'm_053-bin-2'),
                                        3:
                                            Measurement(1226251, 10000,
                                                        'm_053-bin-3'),
                                        4:
                                            Measurement(613126, 10000,
                                                        'm_053-bin-4'),
                                        5:
                                            Measurement(306563, 10000,
                                                        'm_053-bin-5'),
                                    },
                                    impression=Measurement(
                                        17473517, 10000, 'm_054'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2289252, 10000, 'm_055'),
                                    k_reach={
                                        1:
                                            Measurement(1181549, 10000,
                                                        'm_056-bin-1'),
                                        2:
                                            Measurement(590775, 10000,
                                                        'm_056-bin-2'),
                                        3:
                                            Measurement(295388, 10000,
                                                        'm_056-bin-3'),
                                        4:
                                            Measurement(147694, 10000,
                                                        'm_056-bin-4'),
                                        5:
                                            Measurement(73846, 10000,
                                                        'm_056-bin-5'),
                                    },
                                    impression=Measurement(
                                        4236753, 10000, 'm_057'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(4750000, 0, 'm_063'),
                                    k_reach={
                                        1:
                                            Measurement(2451613, 0,
                                                        'm_064-bin-1'),
                                        2:
                                            Measurement(1225807, 0,
                                                        'm_064-bin-2'),
                                        3:
                                            Measurement(612904, 0,
                                                        'm_064-bin-3'),
                                        4:
                                            Measurement(306452, 0,
                                                        'm_064-bin-4'),
                                        5:
                                            Measurement(153224, 0,
                                                        'm_064-bin-5'),
                                    },
                                    impression=Measurement(8733867, 0, 'm_065'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1039801, 0, 'm_066'),
                                    k_reach={
                                        1:
                                            Measurement(536671, 0,
                                                        'm_067-bin-1'),
                                        2:
                                            Measurement(268336, 0,
                                                        'm_067-bin-2'),
                                        3:
                                            Measurement(134168, 0,
                                                        'm_067-bin-3'),
                                        4:
                                            Measurement(67084, 0,
                                                        'm_067-bin-4'),
                                        5:
                                            Measurement(33542, 0,
                                                        'm_067-bin-5'),
                                    },
                                    impression=Measurement(1911893, 0, 'm_068'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(760000, 0, 'm_074'),
                                    k_reach={
                                        1:
                                            Measurement(392258, 0,
                                                        'm_075-bin-1'),
                                        2:
                                            Measurement(196129, 0,
                                                        'm_075-bin-2'),
                                        3:
                                            Measurement(98065, 0,
                                                        'm_075-bin-3'),
                                        4:
                                            Measurement(49033, 0,
                                                        'm_075-bin-4'),
                                        5:
                                            Measurement(24515, 0,
                                                        'm_075-bin-5'),
                                    },
                                    impression=Measurement(1397418, 0, 'm_076'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(192662, 0, 'm_077'),
                                    k_reach={
                                        1: Measurement(99438, 0, 'm_078-bin-1'),
                                        2: Measurement(49719, 0, 'm_078-bin-2'),
                                        3: Measurement(24860, 0, 'm_078-bin-3'),
                                        4: Measurement(12430, 0, 'm_078-bin-4'),
                                        5: Measurement(6215, 0, 'm_078-bin-5'),
                                    },
                                    impression=Measurement(354251, 0, 'm_079'),
                                ),
                            ],
                            edp12: [
                                MeasurementSet(
                                    reach=Measurement(13426464, 10000, 'm_085'),
                                    k_reach={
                                        1:
                                            Measurement(6929788, 10000,
                                                        'm_086-bin-1'),
                                        2:
                                            Measurement(3464894, 10000,
                                                        'm_086-bin-2'),
                                        3:
                                            Measurement(1732447, 10000,
                                                        'm_086-bin-3'),
                                        4:
                                            Measurement(866224, 10000,
                                                        'm_086-bin-4'),
                                        5:
                                            Measurement(433111, 10000,
                                                        'm_086-bin-5'),
                                    },
                                    impression=Measurement(
                                        26215389, 10000, 'm_087'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3278136, 10000, 'm_088'),
                                    k_reach={
                                        1:
                                            Measurement(1691941, 10000,
                                                        'm_089-bin-1'),
                                        2:
                                            Measurement(845971, 10000,
                                                        'm_089-bin-2'),
                                        3:
                                            Measurement(422986, 10000,
                                                        'm_089-bin-3'),
                                        4:
                                            Measurement(211493, 10000,
                                                        'm_089-bin-4'),
                                        5:
                                            Measurement(105745, 10000,
                                                        'm_089-bin-5'),
                                    },
                                    impression=Measurement(
                                        6135862, 10000, 'm_090'),
                                ),
                            ],
                        },
                    ),
                "custom":
                    MetricReport(
                        weekly_cumulative_reaches={
                            edp1: [
                                Measurement(9984642, 10000, 'm_091'),
                                Measurement(12020226, 10000, 'm_092'),
                            ],
                            edp2: [
                                Measurement(5000000, 0, 'm_102'),
                                Measurement(6000000, 0, 'm_103'),
                            ],
                            edp3: [
                                Measurement(800000, 0, 'm_113'),
                                Measurement(1000000, 0, 'm_114'),
                            ],
                            edp123: [
                                Measurement(15799013, 10000, 'm_124'),
                                Measurement(19015392, 10000, 'm_125'),
                            ],
                        },
                        whole_campaign_measurements={
                            edp1:
                                MeasurementSet(
                                    reach=Measurement(12017026, 10000, 'm_093'),
                                    k_reach={
                                        1:
                                            Measurement(6202336, 10000,
                                                        'm_094-bin-1'),
                                        2:
                                            Measurement(3101168, 10000,
                                                        'm_094-bin-2'),
                                        3:
                                            Measurement(1550584, 10000,
                                                        'm_094-bin-3'),
                                        4:
                                            Measurement(775292, 10000,
                                                        'm_094-bin-4'),
                                        5:
                                            Measurement(387646, 10000,
                                                        'm_094-bin-5'),
                                    },
                                    impression=Measurement(
                                        22871159, 10000, 'm_095'),
                                ),
                            edp2:
                                MeasurementSet(
                                    reach=Measurement(6000000, 0, 'm_104'),
                                    k_reach={
                                        1:
                                            Measurement(3096774, 0,
                                                        'm_105-bin-1'),
                                        2:
                                            Measurement(1548387, 0,
                                                        'm_105-bin-2'),
                                        3:
                                            Measurement(774194, 0,
                                                        'm_105-bin-3'),
                                        4:
                                            Measurement(387097, 0,
                                                        'm_105-bin-4'),
                                        5:
                                            Measurement(193548, 0,
                                                        'm_105-bin-5'),
                                    },
                                    impression=Measurement(
                                        11216125, 0, 'm_106'),
                                ),
                            edp3:
                                MeasurementSet(
                                    reach=Measurement(1000000, 0, 'm_115'),
                                    k_reach={
                                        1:
                                            Measurement(516129, 0,
                                                        'm_116-bin-1'),
                                        2:
                                            Measurement(258065, 0,
                                                        'm_116-bin-2'),
                                        3:
                                            Measurement(129033, 0,
                                                        'm_116-bin-3'),
                                        4:
                                            Measurement(64517, 0,
                                                        'm_116-bin-4'),
                                        5:
                                            Measurement(32256, 0,
                                                        'm_116-bin-5'),
                                    },
                                    impression=Measurement(1844136, 0, 'm_117'),
                                ),
                            edp123:
                                MeasurementSet(
                                    reach=Measurement(19030737, 10000, 'm_126'),
                                    k_reach={
                                        1:
                                            Measurement(9822316, 10000,
                                                        'm_127-bin-1'),
                                        2:
                                            Measurement(4911158, 10000,
                                                        'm_127-bin-2'),
                                        3:
                                            Measurement(2455579, 10000,
                                                        'm_127-bin-3'),
                                        4:
                                            Measurement(1227790, 10000,
                                                        'm_127-bin-4'),
                                        5:
                                            Measurement(613894, 10000,
                                                        'm_127-bin-5'),
                                    },
                                    impression=Measurement(
                                        35936915, 10000, 'm_128'),
                                ),
                        },
                        weekly_non_cumulative_measurements={
                            edp1: [
                                MeasurementSet(
                                    reach=Measurement(10000981, 10000, 'm_096'),
                                    k_reach={
                                        1:
                                            Measurement(5161797, 10000,
                                                        'm_097-bin-1'),
                                        2:
                                            Measurement(2580899, 10000,
                                                        'm_097-bin-2'),
                                        3:
                                            Measurement(1290450, 10000,
                                                        'm_097-bin-3'),
                                        4:
                                            Measurement(645225, 10000,
                                                        'm_097-bin-4'),
                                        5:
                                            Measurement(322610, 10000,
                                                        'm_097-bin-5'),
                                    },
                                    impression=Measurement(
                                        18382797, 10000, 'm_098'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(2441042, 10000, 'm_099'),
                                    k_reach={
                                        1:
                                            Measurement(1259893, 10000,
                                                        'm_100-bin-1'),
                                        2:
                                            Measurement(629947, 10000,
                                                        'm_100-bin-2'),
                                        3:
                                            Measurement(314974, 10000,
                                                        'm_100-bin-3'),
                                        4:
                                            Measurement(157487, 10000,
                                                        'm_100-bin-4'),
                                        5:
                                            Measurement(78741, 10000,
                                                        'm_100-bin-5'),
                                    },
                                    impression=Measurement(
                                        4488362, 10000, 'm_101'),
                                ),
                            ],
                            edp2: [
                                MeasurementSet(
                                    reach=Measurement(5000000, 0, 'm_107'),
                                    k_reach={
                                        1:
                                            Measurement(2580645, 0,
                                                        'm_108-bin-1'),
                                        2:
                                            Measurement(1290323, 0,
                                                        'm_108-bin-2'),
                                        3:
                                            Measurement(645162, 0,
                                                        'm_108-bin-3'),
                                        4:
                                            Measurement(322581, 0,
                                                        'm_108-bin-4'),
                                        5:
                                            Measurement(161289, 0,
                                                        'm_108-bin-5'),
                                    },
                                    impression=Measurement(9193546, 0, 'm_109'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(1100000, 0, 'm_110'),
                                    k_reach={
                                        1:
                                            Measurement(567742, 0,
                                                        'm_111-bin-1'),
                                        2:
                                            Measurement(283871, 0,
                                                        'm_111-bin-2'),
                                        3:
                                            Measurement(141936, 0,
                                                        'm_111-bin-3'),
                                        4:
                                            Measurement(70968, 0,
                                                        'm_111-bin-4'),
                                        5:
                                            Measurement(35483, 0,
                                                        'm_111-bin-5'),
                                    },
                                    impression=Measurement(2022579, 0, 'm_112'),
                                ),
                            ],
                            edp3: [
                                MeasurementSet(
                                    reach=Measurement(800000, 0, 'm_118'),
                                    k_reach={
                                        1:
                                            Measurement(412903, 0,
                                                        'm_119-bin-1'),
                                        2:
                                            Measurement(206452, 0,
                                                        'm_119-bin-2'),
                                        3:
                                            Measurement(103226, 0,
                                                        'm_119-bin-3'),
                                        4:
                                            Measurement(51613, 0,
                                                        'm_119-bin-4'),
                                        5:
                                            Measurement(25806, 0,
                                                        'm_119-bin-5'),
                                    },
                                    impression=Measurement(1470967, 0, 'm_120'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(202952, 0, 'm_121'),
                                    k_reach={
                                        1:
                                            Measurement(104749, 0,
                                                        'm_122-bin-1'),
                                        2:
                                            Measurement(52375, 0,
                                                        'm_122-bin-2'),
                                        3:
                                            Measurement(26188, 0,
                                                        'm_122-bin-3'),
                                        4:
                                            Measurement(13094, 0,
                                                        'm_122-bin-4'),
                                        5:
                                            Measurement(6546, 0, 'm_122-bin-5'),
                                    },
                                    impression=Measurement(373169, 0, 'm_123'),
                                ),
                            ],
                            edp123: [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000, 'm_129'),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        'm_130-bin-1'),
                                        2:
                                            Measurement(4082574, 10000,
                                                        'm_130-bin-2'),
                                        3:
                                            Measurement(2041287, 10000,
                                                        'm_130-bin-3'),
                                        4:
                                            Measurement(1020644, 10000,
                                                        'm_130-bin-4'),
                                        5:
                                            Measurement(510321, 10000,
                                                        'm_130-bin-5'),
                                    },
                                    impression=Measurement(
                                        29052805, 10000, 'm_131'),
                                ),
                                MeasurementSet(
                                    reach=Measurement(3751542, 10000, 'm_132'),
                                    k_reach={
                                        1:
                                            Measurement(1936280, 10000,
                                                        'm_133-bin-1'),
                                        2:
                                            Measurement(968140, 10000,
                                                        'm_133-bin-2'),
                                        3:
                                            Measurement(484070, 10000,
                                                        'm_133-bin-3'),
                                        4:
                                            Measurement(242035, 10000,
                                                        'm_133-bin-4'),
                                        5:
                                            Measurement(121017, 10000,
                                                        'm_133-bin-5'),
                                    },
                                    impression=Measurement(
                                        6884110, 10000, 'm_134'),
                                ),
                            ],
                        },
                    ),
            },
            metric_subsets_by_parent={'ami': ['mrc', 'custom']},
            cumulative_inconsistency_allowed_edp_combinations={},
        )

        self.assertEqual(expected_report._metric_reports.keys(),
                         expected_report._metric_reports.keys())
        for key in report._metric_reports.keys():
            self.assertEqual(
                report._metric_reports[key]._weekly_cumulative_reaches,
                expected_report._metric_reports[key]._weekly_cumulative_reaches)
            self.assertEqual(
                report._metric_reports[key]._whole_campaign_measurements,
                expected_report._metric_reports[key].
                _whole_campaign_measurements)
            self.assertEqual(
                report._metric_reports[key]._weekly_non_cumulative_measurements,
                expected_report._metric_reports[key].
                _weekly_non_cumulative_measurements)

    def test_report_summary_v2_is_corrected_successfully(self):
        report_summary_v2 = get_report_summary_v2(
            "src/test/python/wfa/measurement/reporting/postprocessing/tools/sample_report_summary_v2.textproto"
        )
        report_post_processor_result = ReportSummaryV2Processor(
            report_summary_v2, []).process()

        self.assertEqual(
            report_post_processor_result.pre_correction_report_summary_v2,
            report_summary_v2)
        self.assertEqual(report_post_processor_result.status.status_code,
                         StatusCode.SOLUTION_FOUND_WITH_HIGHS)
        self.assertLess(
            report_post_processor_result.status.primal_equality_residual,
            NOISE_CORRECTION_TOLERANCE)
        self.assertLess(
            report_post_processor_result.status.primal_inequality_residual,
            NOISE_CORRECTION_TOLERANCE)
        self.assertEqual(len(report_post_processor_result.updated_measurements),
                         278)

    def test_ami_mrc_exempted_edps_skips_consistency_check(self):
        # A report summary with AMI reach (100) < MRC reach (200), which
        # would normally trigger a correction.
        report_summary_textproto = """
            cmms_measurement_consumer_id: "NsQ4CS3K1to"
            external_report_result_id: 123
            population: 1000
            report_summary_set_results {
                external_reporting_set_result_id: 1
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "reporting_set_id_edp1"
                whole_campaign_result {
                    reach {
                        value: 100
                        standard_deviation: 10000
                        metric: "ami_reach"
                    }
                }
            }
            report_summary_set_results {
                external_reporting_set_result_id: 2
                impression_filter: "mrc"
                set_operation: "union"
                data_providers: "reporting_set_id_edp1"
                whole_campaign_result {
                    reach {
                        value: 200
                        standard_deviation: 0
                        metric: "mrc_reach"
                    }
                }
            }
        """

        report_summary = text_format.Parse(
            report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        # Case 1: No exemption. AMI should be corrected to 200.
        result_no_exemption = ReportSummaryV2Processor(report_summary, []).process()
        self.assertEqual(result_no_exemption.updated_measurements["ami_reach"],
                            200)

        # Case 2: Exemption for edp1. AMI reach remains 100.
        result_with_exemption = ReportSummaryV2Processor(
            report_summary,
            ami_mrc_exempted_reporting_set_ids=["reporting_set_id_edp1"]
        ).process()
        self.assertEqual(result_with_exemption.updated_measurements["ami_reach"],
                            100)


    def test_duplicate_edp_combination_metric_names_back_filled(self):
        # Two ReportSummarySetResults with identical (impression_filter,
        # frozenset(data_providers)) but different metric names -- the shape
        # produced by two composite ReportingSets whose set-expressions permute
        # the same DP list (two-anchor stackedIncrementalReach). The solver
        # sees one bucket (correct), but both RSRs must appear in
        # updated_measurements so the response builder can look each up by its
        # own metric name.
        report_summary_textproto = """
            cmms_measurement_consumer_id: "NsQ4CS3K1to"
            external_report_result_id: 456
            population: 1000
            report_summary_set_results {
                external_reporting_set_result_id: 1
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "edp1"
                data_providers: "edp2"
                whole_campaign_result {
                    reach {
                        value: 500
                        standard_deviation: 0
                        metric: "anchor_dp1_reach"
                    }
                }
            }
            report_summary_set_results {
                external_reporting_set_result_id: 2
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "edp2"
                data_providers: "edp1"
                whole_campaign_result {
                    reach {
                        value: 500
                        standard_deviation: 0
                        metric: "anchor_dp2_reach"
                    }
                }
            }
        """
        report_summary = text_format.Parse(
            report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        result = ReportSummaryV2Processor(report_summary, []).process()

        # Both metric names present, both equal to the solver's single
        # per-bucket solved value. Without the alias back-fill, only the
        # winning name (whichever proto-message ordering the dict-overwrite
        # settled on) would be present and the other lookup would KeyError.
        self.assertIn("anchor_dp1_reach", result.updated_measurements)
        self.assertIn("anchor_dp2_reach", result.updated_measurements)
        self.assertEqual(
            result.updated_measurements["anchor_dp1_reach"],
            result.updated_measurements["anchor_dp2_reach"],
        )
        self.assertEqual(result.updated_measurements["anchor_dp1_reach"], 500)

    def test_three_composites_collapse_all_names_back_filled(self):
        # Three ReportSummarySetResults with identical (impression_filter,
        # edp_combination). Exercises the alias-chain resolution: RSR1 gets
        # displaced by RSR2, RSR2 gets displaced by RSR3, so at record time
        # RSR1's alias must chain through to RSR3 rather than pointing at
        # RSR2 (which is itself no longer in updated_measurements after the
        # solver runs). Without _add_alias resolving through the chain, the
        # single-hop back-fill loop would skip RSR1 (canonical=name2 not in
        # updated_measurements) whenever dict iteration puts the RSR1->RSR2
        # entry before the RSR2->RSR3 entry.
        report_summary_textproto = """
            cmms_measurement_consumer_id: "NsQ4CS3K1to"
            external_report_result_id: 456
            population: 1000
            report_summary_set_results {
                external_reporting_set_result_id: 1
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "edp1"
                data_providers: "edp2"
                whole_campaign_result {
                    reach {
                        value: 500
                        standard_deviation: 0
                        metric: "anchor_dp1_reach"
                    }
                }
            }
            report_summary_set_results {
                external_reporting_set_result_id: 2
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "edp2"
                data_providers: "edp1"
                whole_campaign_result {
                    reach {
                        value: 500
                        standard_deviation: 0
                        metric: "anchor_dp2_reach"
                    }
                }
            }
            report_summary_set_results {
                external_reporting_set_result_id: 3
                impression_filter: "ami"
                set_operation: "union"
                data_providers: "edp1"
                data_providers: "edp2"
                whole_campaign_result {
                    reach {
                        value: 500
                        standard_deviation: 0
                        metric: "anchor_dp3_reach"
                    }
                }
            }
        """
        report_summary = text_format.Parse(
            report_summary_textproto,
            report_summary_v2_pb2.ReportSummaryV2(),
        )

        result = ReportSummaryV2Processor(report_summary, []).process()

        for name in ("anchor_dp1_reach", "anchor_dp2_reach",
                     "anchor_dp3_reach"):
            self.assertIn(name, result.updated_measurements)
            self.assertEqual(result.updated_measurements[name], 500)


    def test_union_only_report_solver_feasibility(self):
        # Regression for the primal-infeasibility in _add_impression_relations_to_spec
        # when a report has only a union RSR (no per-EDP primitives).
        #
        # Previously, the union = sum(per-EDP) equality constraint was emitted with
        # an empty singletons list, degenerating to `union_impression = 0`. Combined
        # with the measured value's own equality, the QP solver saw contradictory
        # equalities on the same variable and returned SOLUTION_NOT_FOUND.
        #
        # This shape corresponds to a BasicReport requesting `reporting_unit.cumulative
        # + reporting_unit.non_cumulative` weekly with no `component` subfield -- a
        # plausible caller shape (weekly cross-publisher aggregate dashboard).
        proto = ('cmms_measurement_consumer_id: "MC1"\n'
                 'external_report_result_id: 456\n'
                 'population: 34288880\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 1\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    data_providers: "edp2"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1454 standard_deviation: 0 metric: "cum_w1_reach" }\n'
                 '    }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 18 }\n'
                 '        }\n'
                 '        reach { value: 5330 standard_deviation: 0 metric: "cum_w2_reach" }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1454 standard_deviation: 0 metric: "nc_w1_reach" }\n'
                 '        impression_count { value: 2126 standard_deviation: 0 metric: "nc_w1_impr" }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 18 }\n'
                 '        }\n'
                 '        reach { value: 4211 standard_deviation: 0 metric: "nc_w2_reach" }\n'
                 '        impression_count { value: 6734 standard_deviation: 0 metric: "nc_w2_impr" }\n'
                 '    }\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 5330 standard_deviation: 0 metric: "wc_reach" }\n'
                 '        impression_count { value: 8860 standard_deviation: 0 metric: "wc_impr" }\n'
                 '    }\n'
                 '}\n')
        report_summary = text_format.Parse(
            proto, report_summary_v2_pb2.ReportSummaryV2()
        )
        result = ReportSummaryV2Processor(report_summary, []).process()
        # Every measurement should be present in updated_measurements with its
        # unchanged value (sigma=0 means no correction).
        self.assertEqual(result.updated_measurements["cum_w1_reach"], 1454)
        self.assertEqual(result.updated_measurements["cum_w2_reach"], 5330)
        self.assertEqual(result.updated_measurements["nc_w1_reach"], 1454)
        self.assertEqual(result.updated_measurements["nc_w2_reach"], 4211)
        self.assertEqual(result.updated_measurements["nc_w1_impr"], 2126)
        self.assertEqual(result.updated_measurements["nc_w2_impr"], 6734)
        # whole_campaign shape exercises the whole_campaign branch of the
        # tightened guard -- with only a union RSR present, the branch's
        # single_edp_subset is empty and must be skipped.
        self.assertEqual(result.updated_measurements["wc_reach"], 5330)
        self.assertEqual(result.updated_measurements["wc_impr"], 8860)



    def test_partial_singleton_cover_whole_campaign_impression_skipped(self):
        # Regression for the partial-cover shape on the whole_campaign branch of
        # _add_impression_relations_to_spec. Pre-fix (empty-only guard) the
        # {edp1, edp2, edp3} whole-campaign impression equality would fire with
        # single_edp_subset = [{edp1}, {edp2}] and silently constrain
        # union_impression = edp1_impression + edp2_impression, which is wrong
        # (edp3's contribution is dropped, and the solver adjusts edp1 and edp2
        # downward to compensate). Post-fix (< len guard) the equality is skipped
        # and every impression measurement flows through unchanged.
        proto = ('cmms_measurement_consumer_id: "MC1"\n'
                 'external_report_result_id: 456\n'
                 'population: 34288880\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 1\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    data_providers: "edp2"\n'
                 '    data_providers: "edp3"\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 12000 standard_deviation: 0 metric: "union_reach" }\n'
                 '        impression_count { value: 30000 standard_deviation: 0 metric: "union_impr" }\n'
                 '    }\n'
                 '}\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 2\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 5000 standard_deviation: 0 metric: "edp1_reach" }\n'
                 '        impression_count { value: 8000 standard_deviation: 0 metric: "edp1_impr" }\n'
                 '    }\n'
                 '}\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 3\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp2"\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 4000 standard_deviation: 0 metric: "edp2_reach" }\n'
                 '        impression_count { value: 7000 standard_deviation: 0 metric: "edp2_impr" }\n'
                 '    }\n'
                 '}\n')
        report_summary = text_format.Parse(
            proto, report_summary_v2_pb2.ReportSummaryV2()
        )
        result = ReportSummaryV2Processor(report_summary, []).process()
        # With sigma=0 and the partial-cover equality skipped, every impression
        # value flows through unchanged. Pre-fix, union_impr would be adjusted
        # down to edp1_impr + edp2_impr = 15000 (not 30000).
        self.assertEqual(result.updated_measurements["union_impr"], 30000)
        self.assertEqual(result.updated_measurements["edp1_impr"], 8000)
        self.assertEqual(result.updated_measurements["edp2_impr"], 7000)

    def test_partial_singleton_cover_weekly_non_cumulative_impression_skipped(self):
        # Regression for the partial-cover shape on the weekly_non_cumulative
        # branch of _add_impression_relations_to_spec (analog of the
        # whole_campaign test above).
        proto = ('cmms_measurement_consumer_id: "MC1"\n'
                 'external_report_result_id: 456\n'
                 'population: 34288880\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 1\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    data_providers: "edp2"\n'
                 '    data_providers: "edp3"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 12000 standard_deviation: 0 metric: "union_nc_reach" }\n'
                 '        impression_count { value: 30000 standard_deviation: 0 metric: "union_nc_impr" }\n'
                 '    }\n'
                 '}\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 2\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 5000 standard_deviation: 0 metric: "edp1_nc_reach" }\n'
                 '        impression_count { value: 8000 standard_deviation: 0 metric: "edp1_nc_impr" }\n'
                 '    }\n'
                 '}\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 3\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp2"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 4000 standard_deviation: 0 metric: "edp2_nc_reach" }\n'
                 '        impression_count { value: 7000 standard_deviation: 0 metric: "edp2_nc_impr" }\n'
                 '    }\n'
                 '}\n')
        report_summary = text_format.Parse(
            proto, report_summary_v2_pb2.ReportSummaryV2()
        )
        result = ReportSummaryV2Processor(report_summary, []).process()
        # Pre-fix, union_nc_impr would be adjusted down to edp1_nc_impr +
        # edp2_nc_impr = 15000 (not 30000). Post-fix, every impression flows
        # through unchanged.
        self.assertEqual(result.updated_measurements["union_nc_impr"], 30000)
        self.assertEqual(result.updated_measurements["edp1_nc_impr"], 8000)
        self.assertEqual(result.updated_measurements["edp2_nc_impr"], 7000)



    def test_full_shape_collapsing_rsrs_all_alias_paths_back_filled(self):
        # Coverage for the alias code paths not exercised by the simpler
        # whole_campaign-reach tests: two RSRs share (impression_filter,
        # frozenset(data_providers)) via permuted data_providers ordering. Each
        # carries cumulative_results (drives _record_measurement_list_aliases,
        # a list-of-reach path), non_cumulative_results with frequency bins
        # (drives the strict=True zip through _record_measurement_set_aliases
        # -- covering k_reach-bin aliasing on multiple bins per week), and
        # whole_campaign_result with frequency bins (drives the whole_campaign
        # _record_measurement_set_aliases branch, including its own k_reach).
        # Impression aliasing is not exercised here because on this branch the
        # pre-#4141 impression equality (union = sum(singles)) degenerates to
        # 0 without per-EDP primitives; #4141 tightens that guard so a follow-
        # up expansion of this test to add impression_count can land there.
        proto = ('cmms_measurement_consumer_id: "MC1"\n'
                 'external_report_result_id: 456\n'
                 'population: 34288880\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 1\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp1"\n'
                 '    data_providers: "edp2"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1000 standard_deviation: 0 metric: "a_cum_w1_reach" }\n'
                 '    }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 22 }\n'
                 '        }\n'
                 '        reach { value: 2500 standard_deviation: 0 metric: "a_cum_w2_reach" }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1000 standard_deviation: 0 metric: "a_nc_w1_reach" }\n'
                 '        frequency {\n'
                 '            metric: "a_nc_w1_freq"\n'
                 '            bins { key: 1 value { value: 600 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 400 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 22 }\n'
                 '        }\n'
                 '        reach { value: 1500 standard_deviation: 0 metric: "a_nc_w2_reach" }\n'
                 '        frequency {\n'
                 '            metric: "a_nc_w2_freq"\n'
                 '            bins { key: 1 value { value: 900 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 600 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 2500 standard_deviation: 0 metric: "a_wc_reach" }\n'
                 '        frequency {\n'
                 '            metric: "a_wc_freq"\n'
                 '            bins { key: 1 value { value: 1500 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 1000 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '}\n'
                 'report_summary_set_results {\n'
                 '    external_reporting_set_result_id: 2\n'
                 '    impression_filter: "ami"\n'
                 '    set_operation: "union"\n'
                 '    data_providers: "edp2"\n'
                 '    data_providers: "edp1"\n'
                 '    metric_frequency_spec { weekly: MONDAY }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1000 standard_deviation: 0 metric: "b_cum_w1_reach" }\n'
                 '    }\n'
                 '    cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 22 }\n'
                 '        }\n'
                 '        reach { value: 2500 standard_deviation: 0 metric: "b_cum_w2_reach" }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 14 }\n'
                 '            end { year: 2021 month: 3 day: 15 }\n'
                 '        }\n'
                 '        reach { value: 1000 standard_deviation: 0 metric: "b_nc_w1_reach" }\n'
                 '        frequency {\n'
                 '            metric: "b_nc_w1_freq"\n'
                 '            bins { key: 1 value { value: 600 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 400 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '    non_cumulative_results {\n'
                 '        key {\n'
                 '            non_cumulative_start { year: 2021 month: 3 day: 15 }\n'
                 '            end { year: 2021 month: 3 day: 22 }\n'
                 '        }\n'
                 '        reach { value: 1500 standard_deviation: 0 metric: "b_nc_w2_reach" }\n'
                 '        frequency {\n'
                 '            metric: "b_nc_w2_freq"\n'
                 '            bins { key: 1 value { value: 900 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 600 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '    whole_campaign_result {\n'
                 '        reach { value: 2500 standard_deviation: 0 metric: "b_wc_reach" }\n'
                 '        frequency {\n'
                 '            metric: "b_wc_freq"\n'
                 '            bins { key: 1 value { value: 1500 standard_deviation: 0 } }\n'
                 '            bins { key: 2 value { value: 1000 standard_deviation: 0 } }\n'
                 '        }\n'
                 '    }\n'
                 '}\n')
        report_summary = text_format.Parse(
            proto, report_summary_v2_pb2.ReportSummaryV2()
        )
        result = ReportSummaryV2Processor(report_summary, []).process()

        # Cumulative reach: exercises _record_measurement_list_aliases. Both
        # a_* and b_* names must be present with equal values.
        for a_name, b_name, expected in [
            ("a_cum_w1_reach", "b_cum_w1_reach", 1000),
            ("a_cum_w2_reach", "b_cum_w2_reach", 2500),
        ]:
            self.assertIn(a_name, result.updated_measurements)
            self.assertIn(b_name, result.updated_measurements)
            self.assertEqual(
                result.updated_measurements[a_name],
                result.updated_measurements[b_name])
            self.assertEqual(result.updated_measurements[a_name], expected)

        # Non-cumulative reach: exercises the strict=True zip through
        # _record_measurement_set_aliases (the reach arm of MeasurementSet).
        for a_name, b_name in [
            ("a_nc_w1_reach", "b_nc_w1_reach"),
            ("a_nc_w2_reach", "b_nc_w2_reach"),
        ]:
            self.assertIn(a_name, result.updated_measurements)
            self.assertIn(b_name, result.updated_measurements)
            self.assertEqual(
                result.updated_measurements[a_name],
                result.updated_measurements[b_name])

        # Non-cumulative frequency-bin aliasing (k_reach-bin path).
        for a_prefix, b_prefix in [("a_nc_w1_freq", "b_nc_w1_freq"),
                                   ("a_nc_w2_freq", "b_nc_w2_freq")]:
            for bin_label in (1, 2):
                a_name = f"{a_prefix}-bin-{bin_label}"
                b_name = f"{b_prefix}-bin-{bin_label}"
                self.assertIn(a_name, result.updated_measurements)
                self.assertIn(b_name, result.updated_measurements)
                self.assertEqual(
                    result.updated_measurements[a_name],
                    result.updated_measurements[b_name])

        # Whole-campaign reach + frequency bins.
        self.assertIn("a_wc_reach", result.updated_measurements)
        self.assertIn("b_wc_reach", result.updated_measurements)
        self.assertEqual(
            result.updated_measurements["a_wc_reach"],
            result.updated_measurements["b_wc_reach"])
        for bin_label in (1, 2):
            a_name = f"a_wc_freq-bin-{bin_label}"
            b_name = f"b_wc_freq-bin-{bin_label}"
            self.assertIn(a_name, result.updated_measurements)
            self.assertIn(b_name, result.updated_measurements)
            self.assertEqual(
                result.updated_measurements[a_name],
                result.updated_measurements[b_name])



def read_file_to_string(filename: str) -> str:
    try:
        with open(filename, 'r') as file:
            return file.read()
    except FileNotFoundError:
        pass


def get_report_summary_v2(filename: str):
    report_summary_textproto = read_file_to_string(filename)
    report_summary = text_format.Parse(report_summary_textproto,
                                       report_summary_v2_pb2.ReportSummaryV2())
    return report_summary


if __name__ == '__main__':
    unittest.main()
