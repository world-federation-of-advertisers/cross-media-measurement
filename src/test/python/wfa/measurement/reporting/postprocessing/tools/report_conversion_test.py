# Copyright 2025 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for report_conversion."""

import unittest

from google.protobuf import text_format
from src.main.python.wfa.measurement.reporting.postprocessing.tools.report_conversion import get_report_summary_v2_from_report_result
from wfa.measurement.internal.reporting.v2 import report_result_pb2

_REPORT_RESULT_TEXTPROTO = """
cmms_measurement_consumer_id: "abcd"
external_report_result_id: 123456
report_start {
  year: 2025
  month: 10
  day: 1
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 1
      cumulative_results {
        reach {
          value: 9992500
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 10008130
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 18379493
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 5165486
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 2582743
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1291372
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 645686
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 322843
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 2
      cumulative_results {
        reach {
          value: 11998422
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 2452001
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 4471035
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1265549
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 632775
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 316388
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 158194
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 79095
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 3
      non_cumulative_results {
        reach {
          value: 11978894
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 22870892
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 6182655
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 3091328
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1545664
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 772832
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 386415
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 4
      cumulative_results {
        reach {
          value: 5000000
        }
      }
      non_cumulative_results {
        reach {
          value: 5000000
        }
        impression_count {
          value: 9193546
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 2580645 }
          }
          bins: {
            key: 2
            value: { value: 1290323 }
          }
          bins: {
            key: 3
            value: { value: 645162 }
          }
          bins: {
            key: 4
            value: { value: 322581 }
          }
          bins: {
            key: 5
            value: { value: 161289 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 5
      cumulative_results {
        reach {
          value: 6000000
        }
      }
      non_cumulative_results {
        reach {
          value: 1100000
        }
        impression_count {
          value: 2022579
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 567742 }
          }
          bins: {
            key: 2
            value: { value: 283871 }
          }
          bins: {
            key: 3
            value: { value: 141936 }
          }
          bins: {
            key: 4
            value: { value: 70968 }
          }
          bins: {
            key: 5
            value: { value: 35483 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 6
      non_cumulative_results {
        reach {
          value: 6000000
        }
        impression_count {
          value: 11216125
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 3096774 }
          }
          bins: {
            key: 2
            value: { value: 1548387 }
          }
          bins: {
            key: 3
            value: { value: 774194 }
          }
          bins: {
            key: 4
            value: { value: 387097 }
          }
          bins: {
            key: 5
            value: { value: 193548 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 7
      cumulative_results {
        reach {
          value: 800000
        }
      }
      non_cumulative_results {
        reach {
          value: 800000
        }
        impression_count {
          value: 1470967
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 412903 }
          }
          bins: {
            key: 2
            value: { value: 206452 }
          }
          bins: {
            key: 3
            value: { value: 103226 }
          }
          bins: {
            key: 4
            value: { value: 51613 }
          }
          bins: {
            key: 5
            value: { value: 25806 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 8
      cumulative_results {
        reach {
          value: 1000000
        }
      }
      non_cumulative_results {
        reach {
          value: 202952
        }
        impression_count {
          value: 373169
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 104749 }
          }
          bins: {
            key: 2
            value: { value: 52375 }
          }
          bins: {
            key: 3
            value: { value: 26188 }
          }
          bins: {
            key: 4
            value: { value: 13094 }
          }
          bins: {
            key: 5
            value: { value: 6546 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 9
      non_cumulative_results {
        reach {
          value: 1000000
        }
        impression_count {
          value: 1844136
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 516129 }
          }
          bins: {
            key: 2
            value: { value: 258065 }
          }
          bins: {
            key: 3
            value: { value: 129033 }
          }
          bins: {
            key: 4
            value: { value: 64517 }
          }
          bins: {
            key: 5
            value: { value: 32256 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 10
      cumulative_results {
        reach {
          value: 15830545
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 15829304
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 29046331
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 8169963
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 4084982
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 2042491
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 1021246
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 510622
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 11
      cumulative_results {
        reach {
          value: 19010669
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 3761510
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 6904573
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1941425
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 970713
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 485357
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 242679
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 121336
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 12
      non_cumulative_results {
        reach {
          value: 19021738
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 35926461
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 9817671
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 4908836
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 2454418
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 1227209
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 613604
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "ami"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 13
      non_cumulative_results {
        reach {
          value: 16686873
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 34113188
          univariate_statistics { standard_deviation: 10000 }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 14
      cumulative_results {
        reach {
          value: 9501618
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 9503446
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 17473517
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 4905004
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 2452502
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1226251
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 613126
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 306563
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 15
      cumulative_results {
        reach {
          value: 11389309
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 2289252
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 4236753
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1181549
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 590775
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 295388
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 147694
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 73846
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 16
      non_cumulative_results {
        reach {
          value: 11382243
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 21696322
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 5874706
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 2937353
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1468677
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 734339
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 367168
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 17
      cumulative_results {
        reach {
          value: 4750000
        }
      }
      non_cumulative_results {
        reach {
          value: 4750000
        }
        impression_count {
          value: 8733867
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 2451613 }
          }
          bins: {
            key: 2
            value: { value: 1225807 }
          }
          bins: {
            key: 3
            value: { value: 612904 }
          }
          bins: {
            key: 4
            value: { value: 306452 }
          }
          bins: {
            key: 5
            value: { value: 153224 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 18
      cumulative_results {
        reach {
          value: 5700000
        }
      }
      non_cumulative_results {
        reach {
          value: 1039801
        }
        impression_count {
          value: 1911893
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 536671 }
          }
          bins: {
            key: 2
            value: { value: 268336 }
          }
          bins: {
            key: 3
            value: { value: 134168 }
          }
          bins: {
            key: 4
            value: { value: 67084 }
          }
          bins: {
            key: 5
            value: { value: 33542 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 19
      non_cumulative_results {
        reach {
          value: 5700000
        }
        impression_count {
          value: 10645760
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 2941935 }
          }
          bins: {
            key: 2
            value: { value: 1470968 }
          }
          bins: {
            key: 3
            value: { value: 735484 }
          }
          bins: {
            key: 4
            value: { value: 367742 }
          }
          bins: {
            key: 5
            value: { value: 183871 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 20
      cumulative_results {
        reach {
          value: 760000
        }
      }
      non_cumulative_results {
        reach {
          value: 760000
        }
        impression_count {
          value: 1397418
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 392258 }
          }
          bins: {
            key: 2
            value: { value: 196129 }
          }
          bins: {
            key: 3
            value: { value: 98065 }
          }
          bins: {
            key: 4
            value: { value: 49033 }
          }
          bins: {
            key: 5
            value: { value: 24515 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 21
      cumulative_results {
        reach {
          value: 950000
        }
      }
      non_cumulative_results {
        reach {
          value: 192662
        }
        impression_count {
          value: 354251
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 99438 }
          }
          bins: {
            key: 2
            value: { value: 49719 }
          }
          bins: {
            key: 3
            value: { value: 24860 }
          }
          bins: {
            key: 4
            value: { value: 12430 }
          }
          bins: {
            key: 5
            value: { value: 6215 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 22
      non_cumulative_results {
        reach {
          value: 950000
        }
        impression_count {
          value: 1751669
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 490323 }
          }
          bins: {
            key: 2
            value: { value: 245162 }
          }
          bins: {
            key: 3
            value: { value: 122581 }
          }
          bins: {
            key: 4
            value: { value: 61291 }
          }
          bins: {
            key: 5
            value: { value: 30643 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 23
      cumulative_results {
        reach {
          value: 13427250
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 13426464
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 26215389
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 6929788
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 3464894
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1732447
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 866224
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 433111
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 24
      cumulative_results {
        reach {
          value: 15920317
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 3278136
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 6135862
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1691941
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 845971
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 422986
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 211493
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 105745
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  external_impression_qualification_filter_id: "mrc"
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 25
      non_cumulative_results {
        reach {
          value: 15908881
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 32337826
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 8211035
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 4105518
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 2052759
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 1026380
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 513189
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 26
      cumulative_results {
        reach {
          value: 9984642
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 10000981
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 18382797
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 5161797
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 2580899
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1290450
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 645225
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 322610
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 27
      cumulative_results {
        reach {
          value: 12020226
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 2441042
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 4488362
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1259893
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 629947
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 314974
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 157487
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 78741
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 28
      non_cumulative_results {
        reach {
          value: 12017026
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 22871159
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 6202336
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 3101168
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 1550584
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 775292
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 387646
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 29
      cumulative_results {
        reach {
          value: 5000000
        }
      }
      non_cumulative_results {
        reach {
          value: 5000000
        }
        impression_count {
          value: 9193546
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 2580645 }
          }
          bins: {
            key: 2
            value: { value: 1290323 }
          }
          bins: {
            key: 3
            value: { value: 645162 }
          }
          bins: {
            key: 4
            value: { value: 322581 }
          }
          bins: {
            key: 5
            value: { value: 161289 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 30
      cumulative_results {
        reach {
          value: 6000000
        }
      }
      non_cumulative_results {
        reach {
          value: 1100000
        }
        impression_count {
          value: 2022579
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 567742 }
          }
          bins: {
            key: 2
            value: { value: 283871 }
          }
          bins: {
            key: 3
            value: { value: 141936 }
          }
          bins: {
            key: 4
            value: { value: 70968 }
          }
          bins: {
            key: 5
            value: { value: 35483 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp2"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 31
      non_cumulative_results {
        reach {
          value: 6000000
        }
        impression_count {
          value: 11216125
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 3096774 }
          }
          bins: {
            key: 2
            value: { value: 1548387 }
          }
          bins: {
            key: 3
            value: { value: 774194 }
          }
          bins: {
            key: 4
            value: { value: 387097 }
          }
          bins: {
            key: 5
            value: { value: 193548 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 32
      cumulative_results {
        reach {
          value: 800000
        }
      }
      non_cumulative_results {
        reach {
          value: 800000
        }
        impression_count {
          value: 1470967
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 412903 }
          }
          bins: {
            key: 2
            value: { value: 206452 }
          }
          bins: {
            key: 3
            value: { value: 103226 }
          }
          bins: {
            key: 4
            value: { value: 51613 }
          }
          bins: {
            key: 5
            value: { value: 25806 }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 33
      cumulative_results {
        reach {
          value: 1000000
        }
      }
      non_cumulative_results {
        reach {
          value: 202952
        }
        impression_count {
          value: 373169
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 104749 }
          }
          bins: {
            key: 2
            value: { value: 52375 }
          }
          bins: {
            key: 3
            value: { value: 26188 }
          }
          bins: {
            key: 4
            value: { value: 13094 }
          }
          bins: {
            key: 5
            value: { value: 6546 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp3"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 34
      non_cumulative_results {
        reach {
          value: 1000000
        }
        impression_count {
          value: 1844136
        }
        frequency_histogram {
          bins: {
            key: 1
            value: { value: 516129 }
          }
          bins: {
            key: 2
            value: { value: 258065 }
          }
          bins: {
            key: 3
            value: { value: 129033 }
          }
          bins: {
            key: 4
            value: { value: 64517 }
          }
          bins: {
            key: 5
            value: { value: 32256 }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: WEEKLY
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 8
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 35
      cumulative_results {
        reach {
          value: 15799013
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 15819974
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 29052805
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 8165148
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 4082574
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 2041287
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 1020644
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 510321
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 8
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 36
      cumulative_results {
        reach {
          value: 19015392
          univariate_statistics { standard_deviation: 10000 }
        }
      }
      non_cumulative_results {
        reach {
          value: 3751542
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 6884110
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 1936280
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 968140
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 484070
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 242035
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 121017
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
reporting_set_results {
  external_reporting_set_id: "edp1_edp2_edp3"
  venn_diagram_region_type: UNION
  custom: true
  metric_frequency_type: TOTAL
  groupings {
    path: "person.age_group"
    value { enum_value: "YEARS_18_TO_34" }
  }
  groupings {
    path: "person.gender"
    value { enum_value: "MALE" }
  }
  population_size: 10000000
  event_filters {
    terms {
      path: "banner_ad.viewable"
      value {
        bool_value: true
      }
    }
  }
  reporting_window_results {
    window_start_date {
      year: 2025
      month: 10
      day: 1
    }
    window_end_date {
      year: 2025
      month: 10
      day: 15
    }
    noisy_report_result_values {
      external_noisy_report_result_value_id: 37
      non_cumulative_results {
        reach {
          value: 19030737
          univariate_statistics { standard_deviation: 10000 }
        }
        impression_count {
          value: 35936915
          univariate_statistics { standard_deviation: 10000 }
        }
        frequency_histogram {
          bins: {
            key: 1
            value: {
              value: 9822316
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 2
            value: {
              value: 4911158
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 3
            value: {
              value: 2455579
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 4
            value: {
              value: 1227790
              univariate_statistics { standard_deviation: 10000 }
            }
          }
          bins: {
            key: 5
            value: {
              value: 613894
              univariate_statistics { standard_deviation: 10000 }
            }
          }
        }
      }
    }
  }
}
"""


class ReportConversionTest(unittest.TestCase):

  def test_get_report_summary_v2_from_empty_report_results(self) -> None:
    report_result = report_result_pb2.ReportResult()
    edp_combinations_by_reporting_set_id = {}
    summaries = get_report_summary_v2_from_report_result(
        report_result, edp_combinations_by_reporting_set_id
    )
    self.assertEqual(summaries, [])

  def test_get_report_summary_v2_from_report_result(self) -> None:
    report_result = text_format.Parse(
        _REPORT_RESULT_TEXTPROTO, report_result_pb2.ReportResult()
    )
    edp_combinations_by_reporting_set_id = {
        'edp1': ['edp1'],
        'edp2': ['edp2'],
        'edp3': ['edp3'],
        'edp1_edp2': ['edp1', 'edp2'],
        'edp1_edp2_edp3': ['edp1', 'edp2', 'edp3'],
    }

    summaries = get_report_summary_v2_from_report_result(
        report_result, edp_combinations_by_reporting_set_id
    )
    print(summaries[0])
    self.assertEqual(len(summaries), 1)



if __name__ == '__main__':
  unittest.main()
