# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from noiseninja.noised_measurements import Measurement
from noiseninja.noised_measurements import MeasurementSet
from noiseninja.noised_measurements import SetMeasurementsSpec

from report.report import MetricReport
from report.report import Report
from report.report import build_measurement_set

from src.main.proto.wfa.measurement.reporting.postprocessing.v2alpha import \
  report_post_processor_result_pb2

StatusCode = report_post_processor_result_pb2.ReportPostProcessorStatus.StatusCode
ReportQuality = report_post_processor_result_pb2.ReportQuality

EXPECTED_PRECISION = 1
EDP_ONE = "EDP_ONE"
EDP_TWO = "EDP_TWO"
EDP_THREE = "EDP_THREE"

SAMPLE_REPORT = Report(
    metric_reports={
        "ami":
            MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(9992500, 10000, "measurement_001"),
                        Measurement(11998422, 10000, "measurement_002")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(5000000, 0, "measurement_003"),
                        Measurement(6000000, 0, "measurement_004")
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(800000, 0, "measurement_005"),
                        Measurement(1000000, 0, "measurement_006")
                    ],
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(15830545, 10000, "measurement_007"),
                        Measurement(19010669, 10000, "measurement_008")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                            Measurement(11978894, 10000, "measurement_009"),
                        frozenset({EDP_TWO}):
                            Measurement(6000000, 0, "measurement_010"),
                        frozenset({EDP_THREE}):
                            Measurement(1000000, 0, "measurement_011"),
                        frozenset({EDP_ONE, EDP_TWO}):
                            Measurement(16686873, 10000, "measurement_012"),
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                            Measurement(19021738, 10000, "measurement_013"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(6182655, 10000, "measurement_014"),
                            2: Measurement(3091328, 10000, "measurement_015"),
                            3: Measurement(1545664, 10000, "measurement_016"),
                            4: Measurement(772832, 10000, "measurement_017"),
                            5: Measurement(386415, 10000, "measurement_018"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(3096774, 0, "measurement_019"),
                            2: Measurement(1548387, 0, "measurement_020"),
                            3: Measurement(774194, 0, "measurement_021"),
                            4: Measurement(387097, 0, "measurement_022"),
                            5: Measurement(193548, 0, "measurement_023"),
                        },
                        frozenset({EDP_THREE}): {
                            1: Measurement(516129, 0, "measurement_024"),
                            2: Measurement(258065, 0, "measurement_025"),
                            3: Measurement(129033, 0, "measurement_026"),
                            4: Measurement(64517, 0, "measurement_027"),
                            5: Measurement(32256, 0, "measurement_028"),
                        },
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): {
                            1: Measurement(9817671, 10000, "measurement_029"),
                            2: Measurement(4908836, 10000, "measurement_030"),
                            3: Measurement(2454418, 10000, "measurement_031"),
                            4: Measurement(1227209, 10000, "measurement_032"),
                            5: Measurement(613604, 10000, "measurement_033"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}):
                            Measurement(22870892, 10000, "measurement_034"),
                        frozenset({EDP_TWO}):
                            Measurement(11216125, 0, "measurement_035"),
                        frozenset({EDP_THREE}):
                            Measurement(1844136, 0, "measurement_036"),
                        frozenset({EDP_ONE, EDP_TWO}):
                            Measurement(34113188, 10000, "measurement_037"),
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                            Measurement(35926461, 10000, "measurement_038"),
                    }),
                weekly_non_cumulative_measurements={
                    frozenset({EDP_ONE}): [
                        MeasurementSet(
                            reach=Measurement(10008130, 10000,
                                              "measurement_039"),
                            k_reach={
                                1:
                                    Measurement(5165486, 10000,
                                                "measurement_040"),
                                2:
                                    Measurement(2582743, 10000,
                                                "measurement_041"),
                                3:
                                    Measurement(1291372, 10000,
                                                "measurement_042"),
                                4:
                                    Measurement(645686, 10000,
                                                "measurement_043"),
                                5:
                                    Measurement(322843, 10000,
                                                "measurement_044"),
                            },
                            impression=Measurement(18379493, 10000,
                                                   "measurement_045"),
                        ),
                        MeasurementSet(
                            reach=Measurement(2452001, 10000,
                                              "measurement_046"),
                            k_reach={
                                1:
                                    Measurement(1265549, 10000,
                                                "measurement_047"),
                                2:
                                    Measurement(632775, 10000,
                                                "measurement_048"),
                                3:
                                    Measurement(316388, 10000,
                                                "measurement_049"),
                                4:
                                    Measurement(158194, 10000,
                                                "measurement_050"),
                                5:
                                    Measurement(79095, 10000,
                                                "measurement_051"),
                            },
                            impression=Measurement(4471035, 10000,
                                                   "measurement_052"),
                        )
                    ],
                    frozenset({EDP_TWO}): [
                        MeasurementSet(
                            reach=Measurement(5000000, 0, "measurement_053"),
                            k_reach={
                                1: Measurement(2580645, 0, "measurement_054"),
                                2: Measurement(1290323, 0, "measurement_055"),
                                3: Measurement(645162, 0, "measurement_056"),
                                4: Measurement(322581, 0, "measurement_057"),
                                5: Measurement(161289, 0, "measurement_058"),
                            },
                            impression=Measurement(9193546, 0,
                                                   "measurement_059"),
                        ),
                        MeasurementSet(
                            reach=Measurement(1100000, 0, "measurement_060"),
                            k_reach={
                                1: Measurement(567742, 0, "measurement_061"),
                                2: Measurement(283871, 0, "measurement_062"),
                                3: Measurement(141936, 0, "measurement_063"),
                                4: Measurement(70968, 0, "measurement_064"),
                                5: Measurement(35483, 0, "measurement_065"),
                            },
                            impression=Measurement(2022579, 0,
                                                   "measurement_066"),
                        )
                    ],
                    frozenset({EDP_THREE}): [
                        MeasurementSet(
                            reach=Measurement(800000, 0, "measurement_067"),
                            k_reach={
                                1: Measurement(412903, 0, "measurement_068"),
                                2: Measurement(206452, 0, "measurement_069"),
                                3: Measurement(103226, 0, "measurement_070"),
                                4: Measurement(51613, 0, "measurement_071"),
                                5: Measurement(25806, 0, "measurement_072"),
                            },
                            impression=Measurement(1470967, 0,
                                                   "measurement_073"),
                        ),
                        MeasurementSet(
                            reach=Measurement(202952, 0, "measurement_074"),
                            k_reach={
                                1: Measurement(104749, 0, "measurement_075"),
                                2: Measurement(52375, 0, "measurement_076"),
                                3: Measurement(26188, 0, "measurement_077"),
                                4: Measurement(13094, 0, "measurement_078"),
                                5: Measurement(6546, 0, "measurement_079"),
                            },
                            impression=Measurement(373169, 0,
                                                   "measurement_080"),
                        )
                    ],
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        MeasurementSet(
                            reach=Measurement(15829304, 10000,
                                              "measurement_081"),
                            k_reach={
                                1:
                                    Measurement(8169963, 10000,
                                                "measurement_082"),
                                2:
                                    Measurement(4084982, 10000,
                                                "measurement_083"),
                                3:
                                    Measurement(2042491, 10000,
                                                "measurement_084"),
                                4:
                                    Measurement(1021246, 10000,
                                                "measurement_085"),
                                5:
                                    Measurement(510622, 10000,
                                                "measurement_086"),
                            },
                            impression=Measurement(29046331, 10000,
                                                   "measurement_087"),
                        ),
                        MeasurementSet(
                            reach=Measurement(3761510, 10000,
                                              "measurement_088"),
                            k_reach={
                                1:
                                    Measurement(1941425, 10000,
                                                "measurement_089"),
                                2:
                                    Measurement(970713, 10000,
                                                "measurement_090"),
                                3:
                                    Measurement(485357, 10000,
                                                "measurement_091"),
                                4:
                                    Measurement(242679, 10000,
                                                "measurement_092"),
                                5:
                                    Measurement(121336, 10000,
                                                "measurement_093"),
                            },
                            impression=Measurement(6904573, 10000,
                                                   "measurement_094"),
                        )
                    ],
                },
            ),
        "mrc":
            MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(9501618, 10000, "measurement_095"),
                        Measurement(11389309, 10000, "measurement_096")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(4750000, 0, "measurement_097"),
                        Measurement(5700000, 0, "measurement_098")
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(760000, 0, "measurement_099"),
                        Measurement(950000, 0, "measurement_100")
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        Measurement(13427250, 10000, "measurement_101"),
                        Measurement(15920317, 10000, "measurement_102")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                            Measurement(11382243, 10000, "measurement_103"),
                        frozenset({EDP_TWO}):
                            Measurement(5700000, 0, "measurement_104"),
                        frozenset({EDP_THREE}):
                            Measurement(950000, 0, "measurement_105"),
                        frozenset({EDP_ONE, EDP_TWO}):
                            Measurement(15908881, 10000, "measurement_106"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(5874706, 10000, "measurement_107"),
                            2: Measurement(2937353, 10000, "measurement_108"),
                            3: Measurement(1468677, 10000, "measurement_109"),
                            4: Measurement(734339, 10000, "measurement_110"),
                            5: Measurement(367168, 10000, "measurement_111"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(2941935, 0, "measurement_112"),
                            2: Measurement(1470968, 0, "measurement_113"),
                            3: Measurement(735484, 0, "measurement_114"),
                            4: Measurement(367742, 0, "measurement_115"),
                            5: Measurement(183871, 0, "measurement_116"),
                        },
                        frozenset({EDP_THREE}): {
                            1: Measurement(490323, 0, "measurement_117"),
                            2: Measurement(245162, 0, "measurement_118"),
                            3: Measurement(122581, 0, "measurement_119"),
                            4: Measurement(61291, 0, "measurement_120"),
                            5: Measurement(30643, 0, "measurement_121"),
                        },
                        frozenset({EDP_ONE, EDP_TWO}): {
                            1: Measurement(8211035, 10000, "measurement_122"),
                            2: Measurement(4105518, 10000, "measurement_123"),
                            3: Measurement(2052759, 10000, "measurement_124"),
                            4: Measurement(1026380, 10000, "measurement_125"),
                            5: Measurement(513189, 10000, "measurement_126"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}):
                            Measurement(21696322, 10000, "measurement_127"),
                        frozenset({EDP_TWO}):
                            Measurement(10645760, 0, "measurement_128"),
                        frozenset({EDP_THREE}):
                            Measurement(1751669, 0, "measurement_129"),
                        frozenset({EDP_ONE, EDP_TWO}):
                            Measurement(32337826, 10000, "measurement_130"),
                    }),
                weekly_non_cumulative_measurements={
                    frozenset({EDP_ONE}): [
                        MeasurementSet(
                            reach=Measurement(9503446, 10000,
                                              "measurement_131"),
                            k_reach={
                                1:
                                    Measurement(4905004, 10000,
                                                "measurement_132"),
                                2:
                                    Measurement(2452502, 10000,
                                                "measurement_133"),
                                3:
                                    Measurement(1226251, 10000,
                                                "measurement_134"),
                                4:
                                    Measurement(613126, 10000,
                                                "measurement_135"),
                                5:
                                    Measurement(306563, 10000,
                                                "measurement_136"),
                            },
                            impression=Measurement(17473517, 10000,
                                                   "measurement_137"),
                        ),
                        MeasurementSet(
                            reach=Measurement(2289252, 10000,
                                              "measurement_138"),
                            k_reach={
                                1:
                                    Measurement(1181549, 10000,
                                                "measurement_139"),
                                2:
                                    Measurement(590775, 10000,
                                                "measurement_140"),
                                3:
                                    Measurement(295388, 10000,
                                                "measurement_141"),
                                4:
                                    Measurement(147694, 10000,
                                                "measurement_142"),
                                5:
                                    Measurement(73846, 10000,
                                                "measurement_143"),
                            },
                            impression=Measurement(4236753, 10000,
                                                   "measurement_144"),
                        )
                    ],
                    frozenset({EDP_TWO}): [
                        MeasurementSet(
                            reach=Measurement(4750000, 0, "measurement_145"),
                            k_reach={
                                1: Measurement(2451613, 0, "measurement_146"),
                                2: Measurement(1225807, 0, "measurement_147"),
                                3: Measurement(612904, 0, "measurement_148"),
                                4: Measurement(306452, 0, "measurement_149"),
                                5: Measurement(153224, 0, "measurement_150"),
                            },
                            impression=Measurement(8733867, 0,
                                                   "measurement_151"),
                        ),
                        MeasurementSet(
                            reach=Measurement(1039801, 0, "measurement_152"),
                            k_reach={
                                1: Measurement(536671, 0, "measurement_153"),
                                2: Measurement(268336, 0, "measurement_154"),
                                3: Measurement(134168, 0, "measurement_155"),
                                4: Measurement(67084, 0, "measurement_156"),
                                5: Measurement(33542, 0, "measurement_157"),
                            },
                            impression=Measurement(1911893, 0,
                                                   "measurement_158"),
                        )
                    ],
                    frozenset({EDP_THREE}): [
                        MeasurementSet(
                            reach=Measurement(760000, 0, "measurement_159"),
                            k_reach={
                                1: Measurement(392258, 0, "measurement_160"),
                                2: Measurement(196129, 0, "measurement_161"),
                                3: Measurement(98065, 0, "measurement_162"),
                                4: Measurement(49033, 0, "measurement_163"),
                                5: Measurement(24515, 0, "measurement_164"),
                            },
                            impression=Measurement(1397418, 0,
                                                   "measurement_165"),
                        ),
                        MeasurementSet(
                            reach=Measurement(192662, 0, "measurement_166"),
                            k_reach={
                                1: Measurement(99438, 0, "measurement_167"),
                                2: Measurement(49719, 0, "measurement_168"),
                                3: Measurement(24860, 0, "measurement_169"),
                                4: Measurement(12430, 0, "measurement_170"),
                                5: Measurement(6215, 0, "measurement_171"),
                            },
                            impression=Measurement(354251, 0,
                                                   "measurement_172"),
                        )
                    ],
                    frozenset({EDP_ONE, EDP_TWO}): [
                        MeasurementSet(
                            reach=Measurement(13426464, 10000,
                                              "measurement_173"),
                            k_reach={
                                1:
                                    Measurement(6929788, 10000,
                                                "measurement_174"),
                                2:
                                    Measurement(3464894, 10000,
                                                "measurement_175"),
                                3:
                                    Measurement(1732447, 10000,
                                                "measurement_176"),
                                4:
                                    Measurement(866224, 10000,
                                                "measurement_177"),
                                5:
                                    Measurement(433111, 10000,
                                                "measurement_178"),
                            },
                            impression=Measurement(26215389, 10000,
                                                   "measurement_179"),
                        ),
                        MeasurementSet(
                            reach=Measurement(3278136, 10000,
                                              "measurement_180"),
                            k_reach={
                                1:
                                    Measurement(1691941, 10000,
                                                "measurement_181"),
                                2:
                                    Measurement(845971, 10000,
                                                "measurement_182"),
                                3:
                                    Measurement(422986, 10000,
                                                "measurement_183"),
                                4:
                                    Measurement(211493, 10000,
                                                "measurement_184"),
                                5:
                                    Measurement(105745, 10000,
                                                "measurement_185"),
                            },
                            impression=Measurement(6135862, 10000,
                                                   "measurement_186"),
                        )
                    ],
                },
            ),
        "custom":
            MetricReport(
                weekly_cumulative_reaches={
                    frozenset({EDP_ONE}): [
                        Measurement(9984642, 10000, "measurement_187"),
                        Measurement(12020226, 10000, "measurement_188")
                    ],
                    frozenset({EDP_TWO}): [
                        Measurement(5000000, 0, "measurement_189"),
                        Measurement(6000000, 0, "measurement_190")
                    ],
                    frozenset({EDP_THREE}): [
                        Measurement(800000, 0, "measurement_191"),
                        Measurement(1000000, 0, "measurement_192")
                    ],
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        Measurement(15799013, 10000, "measurement_193"),
                        Measurement(19015392, 10000, "measurement_194")
                    ],
                },
                whole_campaign_measurements=build_measurement_set(
                    reach={
                        frozenset({EDP_ONE}):
                            Measurement(12017026, 10000, "measurement_195"),
                        frozenset({EDP_TWO}):
                            Measurement(6000000, 0, "measurement_196"),
                        frozenset({EDP_THREE}):
                            Measurement(1000000, 0, "measurement_197"),
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                            Measurement(19030737, 10000, "measurement_198"),
                    },
                    k_reach={
                        frozenset({EDP_ONE}): {
                            1: Measurement(6202336, 10000, "measurement_199"),
                            2: Measurement(3101168, 10000, "measurement_200"),
                            3: Measurement(1550584, 10000, "measurement_201"),
                            4: Measurement(775292, 10000, "measurement_202"),
                            5: Measurement(387646, 10000, "measurement_203"),
                        },
                        frozenset({EDP_TWO}): {
                            1: Measurement(3096774, 0, "measurement_204"),
                            2: Measurement(1548387, 0, "measurement_205"),
                            3: Measurement(774194, 0, "measurement_206"),
                            4: Measurement(387097, 0, "measurement_207"),
                            5: Measurement(193548, 0, "measurement_208"),
                        },
                        frozenset({EDP_THREE}): {
                            1: Measurement(516129, 0, "measurement_209"),
                            2: Measurement(258065, 0, "measurement_210"),
                            3: Measurement(129033, 0, "measurement_211"),
                            4: Measurement(64517, 0, "measurement_212"),
                            5: Measurement(32256, 0, "measurement_213"),
                        },
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): {
                            1: Measurement(9822316, 10000, "measurement_214"),
                            2: Measurement(4911158, 10000, "measurement_215"),
                            3: Measurement(2455579, 10000, "measurement_216"),
                            4: Measurement(1227790, 10000, "measurement_217"),
                            5: Measurement(613894, 10000, "measurement_218"),
                        },
                    },
                    impression={
                        frozenset({EDP_ONE}):
                            Measurement(22871159, 10000, "measurement_219"),
                        frozenset({EDP_TWO}):
                            Measurement(11216125, 0, "measurement_220"),
                        frozenset({EDP_THREE}):
                            Measurement(1844136, 0, "measurement_221"),
                        frozenset({EDP_ONE, EDP_TWO, EDP_THREE}):
                            Measurement(35936915, 10000, "measurement_222"),
                    }),
                weekly_non_cumulative_measurements={
                    frozenset({EDP_ONE}): [
                        MeasurementSet(
                            reach=Measurement(10000981, 10000,
                                              "measurement_223"),
                            k_reach={
                                1:
                                    Measurement(5161797, 10000,
                                                "measurement_224"),
                                2:
                                    Measurement(2580899, 10000,
                                                "measurement_225"),
                                3:
                                    Measurement(1290450, 10000,
                                                "measurement_226"),
                                4:
                                    Measurement(645225, 10000,
                                                "measurement_227"),
                                5:
                                    Measurement(322610, 10000,
                                                "measurement_228"),
                            },
                            impression=Measurement(18382797, 10000,
                                                   "measurement_229"),
                        ),
                        MeasurementSet(
                            reach=Measurement(2441042, 10000,
                                              "measurement_230"),
                            k_reach={
                                1:
                                    Measurement(1259893, 10000,
                                                "measurement_231"),
                                2:
                                    Measurement(629947, 10000,
                                                "measurement_232"),
                                3:
                                    Measurement(314974, 10000,
                                                "measurement_233"),
                                4:
                                    Measurement(157487, 10000,
                                                "measurement_234"),
                                5:
                                    Measurement(78741, 10000,
                                                "measurement_235"),
                            },
                            impression=Measurement(4488362, 10000,
                                                   "measurement_236"),
                        )
                    ],
                    frozenset({EDP_TWO}): [
                        MeasurementSet(
                            reach=Measurement(5000000, 0, "measurement_237"),
                            k_reach={
                                1: Measurement(2580645, 0, "measurement_238"),
                                2: Measurement(1290323, 0, "measurement_239"),
                                3: Measurement(645162, 0, "measurement_240"),
                                4: Measurement(322581, 0, "measurement_241"),
                                5: Measurement(161289, 0, "measurement_242"),
                            },
                            impression=Measurement(9193546, 0,
                                                   "measurement_243"),
                        ),
                        MeasurementSet(
                            reach=Measurement(1100000, 0, "measurement_244"),
                            k_reach={
                                1: Measurement(567742, 0, "measurement_245"),
                                2: Measurement(283871, 0, "measurement_246"),
                                3: Measurement(141936, 0, "measurement_247"),
                                4: Measurement(70968, 0, "measurement_248"),
                                5: Measurement(35483, 0, "measurement_249"),
                            },
                            impression=Measurement(2022579, 0,
                                                   "measurement_250"),
                        )
                    ],
                    frozenset({EDP_THREE}): [
                        MeasurementSet(
                            reach=Measurement(800000, 0, "measurement_251"),
                            k_reach={
                                1: Measurement(412903, 0, "measurement_252"),
                                2: Measurement(206452, 0, "measurement_253"),
                                3: Measurement(103226, 0, "measurement_254"),
                                4: Measurement(51613, 0, "measurement_255"),
                                5: Measurement(25806, 0, "measurement_256"),
                            },
                            impression=Measurement(1470967, 0,
                                                   "measurement_257"),
                        ),
                        MeasurementSet(
                            reach=Measurement(202952, 0, "measurement_258"),
                            k_reach={
                                1: Measurement(104749, 0, "measurement_259"),
                                2: Measurement(52375, 0, "measurement_260"),
                                3: Measurement(26188, 0, "measurement_261"),
                                4: Measurement(13094, 0, "measurement_262"),
                                5: Measurement(6546, 0, "measurement_263"),
                            },
                            impression=Measurement(373169, 0,
                                                   "measurement_264"),
                        )
                    ],
                    frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                        MeasurementSet(
                            reach=Measurement(15819974, 10000,
                                              "measurement_265"),
                            k_reach={
                                1:
                                    Measurement(8165148, 10000,
                                                "measurement_266"),
                                2:
                                    Measurement(4082574, 10000,
                                                "measurement_267"),
                                3:
                                    Measurement(2041287, 10000,
                                                "measurement_268"),
                                4:
                                    Measurement(1020644, 10000,
                                                "measurement_269"),
                                5:
                                    Measurement(510321, 10000,
                                                "measurement_270"),
                            },
                            impression=Measurement(29052805, 10000,
                                                   "measurement_271"),
                        ),
                        MeasurementSet(
                            reach=Measurement(3751542, 10000,
                                              "measurement_272"),
                            k_reach={
                                1:
                                    Measurement(1936280, 10000,
                                                "measurement_273"),
                                2:
                                    Measurement(968140, 10000,
                                                "measurement_274"),
                                3:
                                    Measurement(484070, 10000,
                                                "measurement_275"),
                                4:
                                    Measurement(242035, 10000,
                                                "measurement_276"),
                                5:
                                    Measurement(121017, 10000,
                                                "measurement_277"),
                            },
                            impression=Measurement(6884110, 10000,
                                                   "measurement_278"),
                        )
                    ],
                },
            )
    },
    metric_subsets_by_parent={"ami": ["mrc", "custom"]},
    cumulative_inconsistency_allowed_edp_combinations={},
)


class TestReportMcApi2(unittest.TestCase):

    def test_report_with_inconsistent_number_of_cumulative_periods_raise_exception(
            self):
        with self.assertRaises(ValueError) as cm:
            # ami weekly cumulative measurements have 2 periods, while mrc has 1.
            report = Report(
                metric_reports={
                    "ami":
                        MetricReport(
                            weekly_cumulative_reaches={
                                frozenset({EDP_TWO}): [
                                    Measurement(6000000, 0, "measurement_1"),
                                    Measurement(6000000, 0, "measurement_11")
                                ],
                            },
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={},
                        ),
                    "custom":
                        MetricReport(
                            weekly_cumulative_reaches={
                                frozenset({EDP_THREE}): [
                                    Measurement(1000000, 0, "measurement_6")
                                ],
                            },
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={},
                        ),
                },
                metric_subsets_by_parent={"ami": ["custom"]},
                cumulative_inconsistency_allowed_edp_combinations={},
            )
        self.assertEqual(
            str(cm.exception),
            "All weekly measurements must have the same number of periods.")

    def test_report_with_inconsistent_number_of_cumulative_and_non_cumulative_periods_raise_exception(
            self):
        with self.assertRaises(ValueError) as cm:
            # The weekly cumulative reaches has 2 periods, while the weekly
            # non-cumulative reaches has 1.
            report = Report(
                metric_reports={
                    "ami":
                        MetricReport(
                            weekly_cumulative_reaches={
                                frozenset({EDP_TWO}): [
                                    Measurement(6000000, 0, "measurement_1"),
                                    Measurement(6000000, 0, "measurement_11")
                                ],
                            },
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={},
                        ),
                    "custom":
                        MetricReport(
                            weekly_cumulative_reaches={},
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={
                                frozenset({EDP_ONE}): [
                                    MeasurementSet(
                                        reach=Measurement(
                                            15819974, 10000, "measurement_3"),
                                        k_reach={
                                            1:
                                                Measurement(
                                                    8165148, 10000,
                                                    "measurement_4")
                                        },
                                        impression=Measurement(
                                            29052805, 10000, "measurement_5"),
                                    )
                                ],
                            },
                        ),
                },
                metric_subsets_by_parent={"ami": ["custom"]},
                cumulative_inconsistency_allowed_edp_combinations={},
            )
        self.assertEqual(
            str(cm.exception),
            "All weekly measurements must have the same number of periods.")

    def test_report_with_inconsistent_number_of_k_reach_frequencies_raise_exception(
            self):
        with self.assertRaises(ValueError) as cm:
            # ami frequency histogram has 1 bin, while mrc has 2.
            report = Report(
                metric_reports={
                    "ami":
                        MetricReport(
                            weekly_cumulative_reaches={},
                            whole_campaign_measurements={
                                frozenset({EDP_ONE}):
                                    MeasurementSet(
                                        reach=Measurement(
                                            15819974, 10000, "measurement_6"),
                                        k_reach={
                                            1:
                                                Measurement(
                                                    8165148, 10000,
                                                    "measurement_7"),
                                        },
                                        impression=Measurement(
                                            29052805, 10000, "measurement_9"),
                                    )
                            },
                            weekly_non_cumulative_measurements={},
                        ),
                    "custom":
                        MetricReport(
                            weekly_cumulative_reaches={},
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={
                                frozenset({EDP_ONE}): [
                                    MeasurementSet(
                                        reach=Measurement(
                                            15819974, 10000, "measurement_3"),
                                        k_reach={
                                            1:
                                                Measurement(
                                                    8165148, 10000,
                                                    "measurement_4"),
                                            2:
                                                Measurement(
                                                    8165148, 10000,
                                                    "measurement_8")
                                        },
                                        impression=Measurement(
                                            29052805, 10000, "measurement_5"),
                                    )
                                ],
                            },
                        ),
                },
                metric_subsets_by_parent={"ami": ["custom"]},
                cumulative_inconsistency_allowed_edp_combinations={},
            )
        self.assertEqual(
            str(cm.exception),
            "All k-reach measurements must have the same number of frequencies."
        )

    def test_report_with_inconsistent_number_of_cumulative_periods_raise_exception(
            self):
        with self.assertRaises(ValueError) as cm:
            # ami weekly cumulative measurements have 2 periods, while mrc has 1.
            report = Report(
                metric_reports={
                    "ami":
                        MetricReport(
                            weekly_cumulative_reaches={
                                frozenset({EDP_TWO}): [
                                    Measurement(6000000, 0, "measurement_1"),
                                    Measurement(6000000, 0, "measurement_11")
                                ],
                            },
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={},
                        ),
                    "custom":
                        MetricReport(
                            weekly_cumulative_reaches={
                                frozenset({EDP_THREE}): [
                                    Measurement(1000000, 0, "measurement_6")
                                ],
                            },
                            whole_campaign_measurements={},
                            weekly_non_cumulative_measurements={},
                        ),
                },
                metric_subsets_by_parent={"ami": ["custom"]},
                cumulative_inconsistency_allowed_edp_combinations={},
            )
        self.assertEqual(
            str(cm.exception),
            "All weekly measurements must have the same number of periods.")

    def test_add_weekly_non_cumulative_measurements_to_spec(self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    ),
                "custom":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_THREE}): [
                                Measurement(1000000, 0, "measurement_6")
                            ],
                            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                                Measurement(19015392, 10000, "measurement_7")
                            ],
                        },
                        whole_campaign_measurements={},
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                                MeasurementSet(
                                    reach=Measurement(5819974, 10000,
                                                      "measurement_8"),
                                    k_reach={
                                        1:
                                            Measurement(165148, 10000,
                                                        "measurement_9")
                                    },
                                    impression=Measurement(
                                        9052805, 10000, "measurement_10"),
                                )
                            ],
                        },
                    ),
            },
            metric_subsets_by_parent={"ami": ["custom"]},
            cumulative_inconsistency_allowed_edp_combinations={},
        )

        spec = SetMeasurementsSpec()
        report._add_weekly_non_cumulative_measurements_to_spec(spec)

        measurements_in_spec = {
            m[0].name: m[0] for m in spec._measurements_by_set.values()
        }

        # Verifies that the report has 10 measurements.
        self.assertEqual(report._num_vars, 10)

        # Verifies that the spec has 6 measurements.
        self.assertEqual(len(measurements_in_spec), 6)

        # Verifies the content of the spec.
        self.assertEqual(
            sorted(measurements_in_spec),
            sorted([
                "measurement_3", "measurement_4", "measurement_5",
                "measurement_8", "measurement_9", "measurement_10"
            ]))
        self.assertEqual(measurements_in_spec["measurement_3"].value, 15819974)
        self.assertEqual(measurements_in_spec["measurement_4"].value, 8165148)
        self.assertEqual(measurements_in_spec["measurement_5"].value, 29052805)
        self.assertEqual(measurements_in_spec["measurement_8"].value, 5819974)
        self.assertEqual(measurements_in_spec["measurement_9"].value, 165148)
        self.assertEqual(measurements_in_spec["measurement_10"].value, 9052805)

    def test_add_all_measurements_to_spec_when_report_is_valid(self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    ),
                "custom":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_THREE}): [
                                Measurement(1000000, 0, "measurement_6")
                            ],
                            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                                Measurement(19015392, 10000, "measurement_7")
                            ],
                        },
                        whole_campaign_measurements={},
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE, EDP_TWO, EDP_THREE}): [
                                MeasurementSet(
                                    reach=Measurement(5819974, 10000,
                                                      "measurement_8"),
                                    k_reach={
                                        1:
                                            Measurement(165148, 10000,
                                                        "measurement_9")
                                    },
                                    impression=Measurement(
                                        9052805, 10000, "measurement_10"),
                                )
                            ],
                        },
                    ),
            },
            metric_subsets_by_parent={"ami": ["custom"]},
            cumulative_inconsistency_allowed_edp_combinations={},
        )

        spec = SetMeasurementsSpec()
        report._add_measurements_to_spec(spec)

        measurements_in_spec = {
            m[0].name: m[0] for m in spec._measurements_by_set.values()
        }

        # Verifies that the report has 10 measurements.
        self.assertEqual(report._num_vars, 10)
        self.assertEqual(report._num_periods, 1)
        self.assertEqual(report._num_frequencies, 1)

        # Verifies that the spec has 6 measurements.
        self.assertEqual(len(measurements_in_spec), 10)

        # Verifies the content of the spec.
        self.assertEqual(
            sorted(measurements_in_spec),
            sorted([
                "measurement_1", "measurement_2", "measurement_3",
                "measurement_4", "measurement_5", "measurement_6",
                "measurement_7", "measurement_8", "measurement_9",
                "measurement_10"
            ]))

    def test_get_weekly_cumulative_reaches_return_correct_result(self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    )
            },
            metric_subsets_by_parent={"ami": []},
            cumulative_inconsistency_allowed_edp_combinations={},
        )

        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_cumulative_reach_measurements(frozenset({EDP_TWO})),
            [Measurement(6000000, 0, "measurement_1")])
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_cumulative_reach_measurement(frozenset({EDP_TWO}), 0),
            Measurement(6000000, 0, "measurement_1"))
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_cumulative_reach_measurement(frozenset({EDP_TWO}),
                                                    1), None)
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_cumulative_reach_measurements(frozenset({EDP_ONE})),
            None)
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_cumulative_reach_measurement(frozenset({EDP_ONE}),
                                                    0), None)

    def test_get_weekly_non_cumulative_reaches_return_correct_result(self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    )
            },
            metric_subsets_by_parent={"ami": []},
            cumulative_inconsistency_allowed_edp_combinations={},
        )
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_reach_measurement(frozenset({EDP_ONE}),
                                                        0),
            Measurement(15819974, 10000, "measurement_3"))
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_reach_measurement(frozenset({EDP_ONE}),
                                                        1), None)
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_reach_measurement(frozenset({EDP_TWO}),
                                                        0), None)

    def test_get_weekly_non_cumulative_k_reach_measurements_return_correct_result(
            self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    )
            },
            metric_subsets_by_parent={"ami": []},
            cumulative_inconsistency_allowed_edp_combinations={},
        )
        self.assertEqual(
            list(report._metric_reports["ami"].
                 get_weekly_non_cumulative_k_reach_measurements(
                     frozenset({EDP_ONE}), 0)),
            [Measurement(8165148, 10000, "measurement_4")])
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_k_reach_measurement(
                frozenset({EDP_ONE}), 0, 1),
            Measurement(8165148, 10000, "measurement_4"))
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_k_reach_measurements(
                frozenset({EDP_ONE}), 1), None)
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_k_reach_measurements(
                frozenset({EDP_TWO}), 0), None)

    def test_get_weekly_non_cumulative_impression_measurement_return_correct_result(
            self):
        report = Report(
            metric_reports={
                "ami":
                    MetricReport(
                        weekly_cumulative_reaches={
                            frozenset({EDP_TWO}): [
                                Measurement(6000000, 0, "measurement_1")
                            ],
                        },
                        whole_campaign_measurements=build_measurement_set(
                            reach={
                                frozenset({EDP_ONE}):
                                    Measurement(11978894, 10000,
                                                "measurement_2"),
                            },
                            k_reach={},
                            impression={},
                        ),
                        weekly_non_cumulative_measurements={
                            frozenset({EDP_ONE}): [
                                MeasurementSet(
                                    reach=Measurement(15819974, 10000,
                                                      "measurement_3"),
                                    k_reach={
                                        1:
                                            Measurement(8165148, 10000,
                                                        "measurement_4")
                                    },
                                    impression=Measurement(
                                        29052805, 10000, "measurement_5"),
                                )
                            ],
                        },
                    )
            },
            metric_subsets_by_parent={"ami": []},
            cumulative_inconsistency_allowed_edp_combinations={},
        )
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_impression_measurement(
                frozenset({EDP_ONE}), 0),
            Measurement(29052805, 10000, "measurement_5"))
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_impression_measurement(
                frozenset({EDP_ONE}), 1), None)
        self.assertEqual(
            report._metric_reports["ami"].
            get_weekly_non_cumulative_impression_measurement(
                frozenset({EDP_TWO}), 0), None)

    def test_cover_relations_are_correctly_added_to_spec(self):
        report = SAMPLE_REPORT

        name_to_index = report._measurement_name_to_index

        expected_covers_by_set = {
            # AMI constraints.
            name_to_index["measurement_007"]: [[
                name_to_index["measurement_001"],
                name_to_index["measurement_003"],
                name_to_index["measurement_005"]
            ]],
            name_to_index["measurement_008"]: [[
                name_to_index["measurement_002"],
                name_to_index["measurement_004"],
                name_to_index["measurement_006"]
            ]],
            name_to_index["measurement_012"]: [[
                name_to_index["measurement_009"],
                name_to_index["measurement_010"]
            ]],
            name_to_index["measurement_013"]:
                [[
                    name_to_index["measurement_011"],
                    name_to_index["measurement_012"]
                ],
                 [
                     name_to_index["measurement_009"],
                     name_to_index["measurement_010"],
                     name_to_index["measurement_011"]
                 ],
                 [
                     name_to_index["measurement_009"],
                     name_to_index["measurement_011"],
                     name_to_index["measurement_012"]
                 ],
                 [
                     name_to_index["measurement_010"],
                     name_to_index["measurement_011"],
                     name_to_index["measurement_012"]
                 ],
                 [
                     name_to_index["measurement_009"],
                     name_to_index["measurement_010"],
                     name_to_index["measurement_011"],
                     name_to_index["measurement_012"]
                 ]],
            name_to_index["measurement_081"]: [[
                name_to_index["measurement_039"],
                name_to_index["measurement_053"],
                name_to_index["measurement_067"]
            ]],
            name_to_index["measurement_088"]: [[
                name_to_index["measurement_046"],
                name_to_index["measurement_060"],
                name_to_index["measurement_074"]
            ]],
            # MRC constraints.
            name_to_index["measurement_101"]: [[
                name_to_index["measurement_095"],
                name_to_index["measurement_097"]
            ]],
            name_to_index["measurement_102"]: [[
                name_to_index["measurement_096"],
                name_to_index["measurement_098"]
            ]],
            name_to_index["measurement_106"]: [[
                name_to_index["measurement_103"],
                name_to_index["measurement_104"]
            ]],
            name_to_index["measurement_173"]: [[
                name_to_index["measurement_131"],
                name_to_index["measurement_145"]
            ]],
            name_to_index["measurement_180"]: [[
                name_to_index["measurement_138"],
                name_to_index["measurement_152"]
            ]],
            # CUSTOM constraints.
            name_to_index["measurement_193"]: [[
                name_to_index["measurement_187"],
                name_to_index["measurement_189"],
                name_to_index["measurement_191"]
            ]],
            name_to_index["measurement_194"]: [[
                name_to_index["measurement_188"],
                name_to_index["measurement_190"],
                name_to_index["measurement_192"]
            ]],
            name_to_index["measurement_198"]: [[
                name_to_index["measurement_195"],
                name_to_index["measurement_196"],
                name_to_index["measurement_197"]
            ]],
            name_to_index["measurement_265"]: [[
                name_to_index["measurement_223"],
                name_to_index["measurement_237"],
                name_to_index["measurement_251"]
            ]],
            name_to_index["measurement_272"]: [[
                name_to_index["measurement_230"],
                name_to_index["measurement_244"],
                name_to_index["measurement_258"]
            ]],
        }

        spec = SetMeasurementsSpec()
        report._add_measurements_to_spec(spec)
        report._add_cover_relations_to_spec(spec)
        self.assertEqual(report._num_periods, 2)
        self.assertEqual(report._num_frequencies, 5)
        self.assertEqual(report._num_vars, 278)
        self.assertEqual(len(spec._measurements_by_set), 278)

        self.assertEqual(len(spec._subsets_by_set), 0)
        self.assertEqual(len(spec._equal_sets), 0)
        self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
        self.assertEqual(expected_covers_by_set.keys(),
                         spec._covers_by_set.keys())
        for key in spec._covers_by_set.keys():
            self.assertEqual(
                {
                    tuple(sorted(inner_list))
                    for inner_list in expected_covers_by_set[key]
                }, {
                    tuple(sorted(inner_list))
                    for inner_list in spec._covers_by_set[key]
                })

    def test_subset_relations_are_correctly_added_to_spec(self):
        report = SAMPLE_REPORT

        name_to_index = report._measurement_name_to_index
        expected_subsets_by_set = {
            # AMI constraints.
            name_to_index["measurement_007"]: [
                name_to_index["measurement_001"],
                name_to_index["measurement_003"],
                name_to_index["measurement_005"]
            ],
            name_to_index["measurement_008"]: [
                name_to_index["measurement_002"],
                name_to_index["measurement_004"],
                name_to_index["measurement_006"]
            ],
            name_to_index["measurement_012"]: [
                name_to_index["measurement_009"],
                name_to_index["measurement_010"]
            ],
            name_to_index["measurement_013"]: [
                name_to_index["measurement_009"],
                name_to_index["measurement_010"],
                name_to_index["measurement_011"],
                name_to_index["measurement_012"]
            ],
            name_to_index["measurement_081"]: [
                name_to_index["measurement_039"],
                name_to_index["measurement_053"],
                name_to_index["measurement_067"]
            ],
            name_to_index["measurement_088"]: [
                name_to_index["measurement_046"],
                name_to_index["measurement_060"],
                name_to_index["measurement_074"]
            ],
            # MRC constraints.
            name_to_index["measurement_101"]: [
                name_to_index["measurement_095"],
                name_to_index["measurement_097"]
            ],
            name_to_index["measurement_102"]: [
                name_to_index["measurement_096"],
                name_to_index["measurement_098"]
            ],
            name_to_index["measurement_106"]: [
                name_to_index["measurement_103"],
                name_to_index["measurement_104"]
            ],
            name_to_index["measurement_173"]: [
                name_to_index["measurement_131"],
                name_to_index["measurement_145"]
            ],
            name_to_index["measurement_180"]: [
                name_to_index["measurement_138"],
                name_to_index["measurement_152"]
            ],
            # CUSTOM constraints.
            name_to_index["measurement_193"]: [
                name_to_index["measurement_187"],
                name_to_index["measurement_189"],
                name_to_index["measurement_191"]
            ],
            name_to_index["measurement_194"]: [
                name_to_index["measurement_188"],
                name_to_index["measurement_190"],
                name_to_index["measurement_192"]
            ],
            name_to_index["measurement_198"]: [
                name_to_index["measurement_195"],
                name_to_index["measurement_196"],
                name_to_index["measurement_197"]
            ],
            name_to_index["measurement_265"]: [
                name_to_index["measurement_223"],
                name_to_index["measurement_237"],
                name_to_index["measurement_251"]
            ],
            name_to_index["measurement_272"]: [
                name_to_index["measurement_230"],
                name_to_index["measurement_244"],
                name_to_index["measurement_258"]
            ],
        }

        spec = SetMeasurementsSpec()
        report._add_subset_relations_to_spec(spec)

        self.assertEqual(len(spec._equal_sets), 0)
        self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)
        self.assertEqual(len(spec._covers_by_set), 0)
        self.assertEqual(expected_subsets_by_set.keys(),
                         spec._subsets_by_set.keys())
        for key in spec._subsets_by_set.keys():
            self.assertEqual(sorted(expected_subsets_by_set[key]),
                             sorted(spec._subsets_by_set[key]))

    def test_k_reach_and_reach_relations_are_correctly_added_to_spec(self):
        report = SAMPLE_REPORT
        name_to_index = report._measurement_name_to_index

        expected_equal_sets = [
            # AMI constraints.
            # Whole Campaign
            [
                name_to_index["measurement_009"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(14, 19)]
            ],
            [
                name_to_index["measurement_010"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(19, 24)]
            ],
            [
                name_to_index["measurement_011"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(24, 29)]
            ],
            [
                name_to_index["measurement_013"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(29, 34)]
            ],
            # Weekly Non-Cumulative - Period 1
            [
                name_to_index["measurement_039"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(40, 45)]
            ],
            [
                name_to_index["measurement_053"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(54, 59)]
            ],
            [
                name_to_index["measurement_067"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(68, 73)]
            ],
            [
                name_to_index["measurement_081"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(82, 87)]
            ],
            # Weekly Non-Cumulative - Period 2
            [
                name_to_index["measurement_046"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(47, 52)]
            ],
            [
                name_to_index["measurement_060"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(61, 66)]
            ],
            [
                name_to_index["measurement_074"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(75, 80)]
            ],
            [
                name_to_index["measurement_088"],
                [name_to_index[f"measurement_{i:03d}"] for i in range(89, 94)]
            ],
            # MRC constraints.
            # Whole Campaign
            [
                name_to_index["measurement_103"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(107, 112)
                ]
            ],
            [
                name_to_index["measurement_104"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(112, 117)
                ]
            ],
            [
                name_to_index["measurement_105"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(117, 122)
                ]
            ],
            [
                name_to_index["measurement_106"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(122, 127)
                ]
            ],
            # Weekly Non-Cumulative - Period 1
            [
                name_to_index["measurement_131"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(132, 137)
                ]
            ],
            [
                name_to_index["measurement_145"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(146, 151)
                ]
            ],
            [
                name_to_index["measurement_159"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(160, 165)
                ]
            ],
            [
                name_to_index["measurement_173"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(174, 179)
                ]
            ],
            # Weekly Non-Cumulative - Period 2
            [
                name_to_index["measurement_138"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(139, 144)
                ]
            ],
            [
                name_to_index["measurement_152"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(153, 158)
                ]
            ],
            [
                name_to_index["measurement_166"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(167, 172)
                ]
            ],
            [
                name_to_index["measurement_180"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(181, 186)
                ]
            ],
            # CUSTOM constraints.
            # Whole Campaign
            [
                name_to_index["measurement_195"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(199, 204)
                ]
            ],
            [
                name_to_index["measurement_196"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(204, 209)
                ]
            ],
            [
                name_to_index["measurement_197"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(209, 214)
                ]
            ],
            [
                name_to_index["measurement_198"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(214, 219)
                ]
            ],
            # Weekly Non-Cumulative - Period 1
            [
                name_to_index["measurement_223"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(224, 229)
                ]
            ],
            [
                name_to_index["measurement_237"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(238, 243)
                ]
            ],
            [
                name_to_index["measurement_251"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(252, 257)
                ]
            ],
            [
                name_to_index["measurement_265"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(266, 271)
                ]
            ],
            # Weekly Non-Cumulative - Period 2
            [
                name_to_index["measurement_230"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(231, 236)
                ]
            ],
            [
                name_to_index["measurement_244"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(245, 250)
                ]
            ],
            [
                name_to_index["measurement_258"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(259, 264)
                ]
            ],
            [
                name_to_index["measurement_272"],
                [
                    name_to_index[f"measurement_{i:03d}"]
                    for i in range(273, 278)
                ]
            ],
        ]

        spec = SetMeasurementsSpec()
        report._add_k_reach_and_reach_relations_to_spec(spec)

        self.assertEqual(len(spec._covers_by_set), 0)
        self.assertEqual(len(spec._subsets_by_set), 0)
        self.assertEqual(len(spec._weighted_sum_upperbound_sets), 0)

        # Sort the inner lists for comparison to be order-independent.
        actual_equal_sets = [[s[0], sorted(s[1])] for s in spec._equal_sets]
        for expected_set in expected_equal_sets:
            expected_set[1].sort()
        self.assertCountEqual(actual_equal_sets, expected_equal_sets)


if __name__ == "__main__":
    unittest.main()
