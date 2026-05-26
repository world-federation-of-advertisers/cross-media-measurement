// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var DESCRIPTOR_reporting = {
  "nested": {
    "wfa": {
      "nested": {
        "measurement": {
          "nested": {
            "internal": {
              "nested": {
                "reporting": {
                  "nested": {
                    "v2": {
                      "options": {
                        "java_package": "org.wfanet.measurement.internal.reporting.v2",
                        "java_multiple_files": true
                      },
                      "nested": {
                        "BasicReportDetails": {
                          "fields": {
                            "title": {
                              "type": "string",
                              "id": 1
                            },
                            "impressionQualificationFilters": {
                              "rule": "repeated",
                              "type": "ReportingImpressionQualificationFilter",
                              "id": 2
                            },
                            "effectiveImpressionQualificationFilters": {
                              "rule": "repeated",
                              "type": "ReportingImpressionQualificationFilter",
                              "id": 5
                            },
                            "reportingInterval": {
                              "type": "ReportingInterval",
                              "id": 3
                            },
                            "resultGroupSpecs": {
                              "rule": "repeated",
                              "type": "ResultGroupSpec",
                              "id": 4
                            }
                          }
                        },
                        "ReportingImpressionQualificationFilter": {
                          "fields": {
                            "externalImpressionQualificationFilterId": {
                              "type": "string",
                              "id": 1
                            },
                            "filterSpecs": {
                              "rule": "repeated",
                              "type": "ImpressionQualificationFilterSpec",
                              "id": 2
                            }
                          }
                        },
                        "ImpressionQualificationFilterSpec": {
                          "fields": {
                            "mediaType": {
                              "type": "MediaType",
                              "id": 1
                            },
                            "filters": {
                              "rule": "repeated",
                              "type": "EventFilter",
                              "id": 2
                            }
                          },
                          "nested": {
                            "MediaType": {
                              "values": {
                                "MEDIA_TYPE_UNSPECIFIED": 0,
                                "VIDEO": 1,
                                "DISPLAY": 2,
                                "OTHER": 3
                              }
                            }
                          }
                        },
                        "EventFilter": {
                          "fields": {
                            "terms": {
                              "rule": "repeated",
                              "type": "EventTemplateField",
                              "id": 1
                            }
                          }
                        },
                        "EventTemplateField": {
                          "fields": {
                            "path": {
                              "type": "string",
                              "id": 1
                            },
                            "value": {
                              "type": "FieldValue",
                              "id": 2
                            }
                          },
                          "nested": {
                            "FieldValue": {
                              "oneofs": {
                                "selector": {
                                  "oneof": [
                                    "stringValue",
                                    "enumValue",
                                    "boolValue",
                                    "floatValue"
                                  ]
                                }
                              },
                              "fields": {
                                "stringValue": {
                                  "type": "string",
                                  "id": 1
                                },
                                "enumValue": {
                                  "type": "string",
                                  "id": 2
                                },
                                "boolValue": {
                                  "type": "bool",
                                  "id": 3
                                },
                                "floatValue": {
                                  "type": "float",
                                  "id": 4
                                }
                              }
                            }
                          }
                        },
                        "ReportingInterval": {
                          "oneofs": {
                            "reportStartTime": {
                              "oneof": [
                                "reportStart",
                                "reportStartDate"
                              ]
                            }
                          },
                          "fields": {
                            "reportStart": {
                              "type": "google.type.DateTime",
                              "id": 1
                            },
                            "reportStartDate": {
                              "type": "google.type.Date",
                              "id": 4
                            },
                            "effectiveReportStart": {
                              "type": "google.type.DateTime",
                              "id": 3
                            },
                            "reportEnd": {
                              "type": "google.type.Date",
                              "id": 2
                            }
                          }
                        },
                        "DimensionSpec": {
                          "fields": {
                            "grouping": {
                              "type": "Grouping",
                              "id": 3
                            },
                            "filters": {
                              "rule": "repeated",
                              "type": "EventFilter",
                              "id": 4
                            }
                          },
                          "nested": {
                            "Grouping": {
                              "fields": {
                                "eventTemplateFields": {
                                  "rule": "repeated",
                                  "type": "string",
                                  "id": 1
                                }
                              }
                            }
                          }
                        },
                        "ResultGroupMetricSpec": {
                          "fields": {
                            "populationSize": {
                              "type": "bool",
                              "id": 1
                            },
                            "reportingUnit": {
                              "type": "ReportingUnitMetricSetSpec",
                              "id": 2
                            },
                            "component": {
                              "type": "ComponentMetricSetSpec",
                              "id": 3
                            },
                            "componentIntersection": {
                              "type": "ComponentIntersectionMetricSetSpec",
                              "id": 4
                            }
                          },
                          "nested": {
                            "BasicMetricSetSpec": {
                              "fields": {
                                "reach": {
                                  "type": "bool",
                                  "id": 1
                                },
                                "percentReach": {
                                  "type": "bool",
                                  "id": 2
                                },
                                "kPlusReach": {
                                  "type": "int32",
                                  "id": 3
                                },
                                "percentKPlusReach": {
                                  "type": "bool",
                                  "id": 4
                                },
                                "averageFrequency": {
                                  "type": "bool",
                                  "id": 5
                                },
                                "impressions": {
                                  "type": "bool",
                                  "id": 6
                                },
                                "grps": {
                                  "type": "bool",
                                  "id": 7
                                }
                              }
                            },
                            "ReportingUnitMetricSetSpec": {
                              "fields": {
                                "nonCumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 1
                                },
                                "cumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 2
                                },
                                "stackedIncrementalReach": {
                                  "type": "bool",
                                  "id": 3
                                }
                              }
                            },
                            "UniqueMetricSetSpec": {
                              "fields": {
                                "reach": {
                                  "type": "bool",
                                  "id": 1
                                }
                              }
                            },
                            "ComponentMetricSetSpec": {
                              "fields": {
                                "nonCumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 1
                                },
                                "cumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 2
                                },
                                "nonCumulativeUnique": {
                                  "type": "UniqueMetricSetSpec",
                                  "id": 3
                                },
                                "cumulativeUnique": {
                                  "type": "UniqueMetricSetSpec",
                                  "id": 4
                                }
                              }
                            },
                            "ComponentIntersectionMetricSetSpec": {
                              "fields": {
                                "contributorCount": {
                                  "rule": "repeated",
                                  "type": "int32",
                                  "id": 1
                                },
                                "nonCumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 2
                                },
                                "cumulative": {
                                  "type": "BasicMetricSetSpec",
                                  "id": 3
                                }
                              }
                            }
                          }
                        },
                        "ResultGroupSpec": {
                          "fields": {
                            "title": {
                              "type": "string",
                              "id": 1
                            },
                            "reportingUnit": {
                              "type": "ReportingUnit",
                              "id": 2
                            },
                            "metricFrequency": {
                              "type": "MetricFrequencySpec",
                              "id": 3
                            },
                            "dimensionSpec": {
                              "type": "DimensionSpec",
                              "id": 4
                            },
                            "resultGroupMetricSpec": {
                              "type": "ResultGroupMetricSpec",
                              "id": 5
                            }
                          }
                        },
                        "MetricFrequencySpec": {
                          "oneofs": {
                            "selector": {
                              "oneof": [
                                "weekly",
                                "total"
                              ]
                            }
                          },
                          "fields": {
                            "weekly": {
                              "type": "google.type.DayOfWeek",
                              "id": 1
                            },
                            "total": {
                              "type": "bool",
                              "id": 2
                            }
                          }
                        },
                        "DataProviderKey": {
                          "fields": {
                            "cmmsDataProviderId": {
                              "type": "string",
                              "id": 1
                            }
                          }
                        },
                        "ReportingSetKey": {
                          "fields": {
                            "cmmsMeasurementConsumerId": {
                              "type": "string",
                              "id": 1
                            },
                            "externalReportingSetId": {
                              "type": "string",
                              "id": 2
                            }
                          }
                        },
                        "ReportingUnit": {
                          "oneofs": {
                            "Components": {
                              "oneof": [
                                "dataProviderKeys",
                                "reportingSet_Keys"
                              ]
                            }
                          },
                          "fields": {
                            "dataProviderKeys": {
                              "type": "DataProviderKeys",
                              "id": 1
                            },
                            "reportingSet_Keys": {
                              "type": "ReportingSetKeys",
                              "id": 2
                            }
                          },
                          "nested": {
                            "DataProviderKeys": {
                              "fields": {
                                "dataProviderKeys": {
                                  "rule": "repeated",
                                  "type": "DataProviderKey",
                                  "id": 1
                                }
                              }
                            },
                            "ReportingSetKeys": {
                              "fields": {
                                "reportingSetKeys": {
                                  "rule": "repeated",
                                  "type": "ReportingSetKey",
                                  "id": 1
                                }
                              }
                            }
                          }
                        },
                        "BasicReportResultDetails": {
                          "fields": {
                            "resultGroups": {
                              "rule": "repeated",
                              "type": "ResultGroup",
                              "id": 1
                            }
                          }
                        },
                        "ResultGroup": {
                          "fields": {
                            "title": {
                              "type": "string",
                              "id": 1
                            },
                            "results": {
                              "rule": "repeated",
                              "type": "Result",
                              "id": 2
                            }
                          },
                          "nested": {
                            "MetricMetadata": {
                              "fields": {
                                "reportingUnitSummary": {
                                  "type": "ReportingUnitSummary",
                                  "id": 1
                                },
                                "nonCumulativeMetricStartTime": {
                                  "type": "google.protobuf.Timestamp",
                                  "id": 2
                                },
                                "cumulativeMetricStartTime": {
                                  "type": "google.protobuf.Timestamp",
                                  "id": 3
                                },
                                "metricEndTime": {
                                  "type": "google.protobuf.Timestamp",
                                  "id": 4
                                },
                                "metricFrequencySpec": {
                                  "type": "MetricFrequencySpec",
                                  "id": 5
                                },
                                "dimensionSpecSummary": {
                                  "type": "DimensionSpecSummary",
                                  "id": 6
                                },
                                "filter": {
                                  "type": "ReportingImpressionQualificationFilter",
                                  "id": 7
                                }
                              },
                              "nested": {
                                "ReportingUnitComponentSummary": {
                                  "fields": {
                                    "cmmsDataProviderId": {
                                      "type": "string",
                                      "id": 1
                                    },
                                    "cmmsDataProviderDisplayName": {
                                      "type": "string",
                                      "id": 2
                                    },
                                    "eventGroupSummaries": {
                                      "rule": "repeated",
                                      "type": "EventGroupSummary",
                                      "id": 3
                                    }
                                  },
                                  "nested": {
                                    "EventGroupSummary": {
                                      "fields": {
                                        "cmmsMeasurementConsumerId": {
                                          "type": "string",
                                          "id": 1
                                        },
                                        "cmmsEventGroupId": {
                                          "type": "string",
                                          "id": 2
                                        }
                                      }
                                    }
                                  }
                                },
                                "ReportingUnitSummary": {
                                  "fields": {
                                    "reportingUnitComponentSummary": {
                                      "rule": "repeated",
                                      "type": "ReportingUnitComponentSummary",
                                      "id": 1
                                    }
                                  }
                                },
                                "DimensionSpecSummary": {
                                  "fields": {
                                    "groupings": {
                                      "rule": "repeated",
                                      "type": "EventTemplateField",
                                      "id": 1
                                    },
                                    "filters": {
                                      "rule": "repeated",
                                      "type": "EventFilter",
                                      "id": 2
                                    }
                                  }
                                }
                              }
                            },
                            "MetricSet": {
                              "fields": {
                                "populationSize": {
                                  "type": "int64",
                                  "id": 1
                                },
                                "reportingUnit": {
                                  "type": "ReportingUnitMetricSet",
                                  "id": 2
                                },
                                "components": {
                                  "rule": "repeated",
                                  "type": "DataProviderComponentMetricSetMapEntry",
                                  "id": 3
                                },
                                "componentIntersections": {
                                  "rule": "repeated",
                                  "type": "DataProviderComponentIntersectionMetricSet",
                                  "id": 4
                                }
                              },
                              "nested": {
                                "BasicMetricSet": {
                                  "fields": {
                                    "reach": {
                                      "type": "int64",
                                      "id": 1
                                    },
                                    "percentReach": {
                                      "type": "float",
                                      "id": 2
                                    },
                                    "kPlusReach": {
                                      "rule": "repeated",
                                      "type": "int64",
                                      "id": 3
                                    },
                                    "percentKPlusReach": {
                                      "rule": "repeated",
                                      "type": "float",
                                      "id": 4
                                    },
                                    "averageFrequency": {
                                      "type": "float",
                                      "id": 5
                                    },
                                    "impressions": {
                                      "type": "int64",
                                      "id": 6
                                    },
                                    "grps": {
                                      "type": "float",
                                      "id": 7
                                    }
                                  }
                                },
                                "ReportingUnitMetricSet": {
                                  "fields": {
                                    "nonCumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 1
                                    },
                                    "cumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 2
                                    },
                                    "stackedIncrementalReach": {
                                      "rule": "repeated",
                                      "type": "int64",
                                      "id": 3
                                    }
                                  }
                                },
                                "UniqueMetricSet": {
                                  "fields": {
                                    "reach": {
                                      "type": "int64",
                                      "id": 1
                                    }
                                  }
                                },
                                "ComponentMetricSet": {
                                  "fields": {
                                    "nonCumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 1
                                    },
                                    "cumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 2
                                    },
                                    "nonCumulativeUnique": {
                                      "type": "UniqueMetricSet",
                                      "id": 4
                                    },
                                    "cumulativeUnique": {
                                      "type": "UniqueMetricSet",
                                      "id": 5
                                    }
                                  },
                                  "reserved": [
                                    [
                                      3,
                                      3
                                    ]
                                  ]
                                },
                                "DataProviderComponentMetricSetMapEntry": {
                                  "fields": {
                                    "key": {
                                      "type": "string",
                                      "id": 1
                                    },
                                    "value": {
                                      "type": "ComponentMetricSet",
                                      "id": 2
                                    }
                                  }
                                },
                                "DataProviderComponentIntersectionMetricSet": {
                                  "fields": {
                                    "cmmsDataProviderIds": {
                                      "rule": "repeated",
                                      "type": "string",
                                      "id": 1
                                    },
                                    "nonCumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 2
                                    },
                                    "cumulative": {
                                      "type": "BasicMetricSet",
                                      "id": 3
                                    }
                                  }
                                }
                              }
                            },
                            "Result": {
                              "fields": {
                                "metadata": {
                                  "type": "MetricMetadata",
                                  "id": 1
                                },
                                "metricSet": {
                                  "type": "MetricSet",
                                  "id": 2
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "google": {
      "nested": {
        "protobuf": {
          "nested": {
            "Duration": {
              "fields": {
                "seconds": {
                  "type": "int64",
                  "id": 1
                },
                "nanos": {
                  "type": "int32",
                  "id": 2
                }
              }
            },
            "Timestamp": {
              "fields": {
                "seconds": {
                  "type": "int64",
                  "id": 1
                },
                "nanos": {
                  "type": "int32",
                  "id": 2
                }
              }
            }
          }
        },
        "type": {
          "options": {
            "cc_enable_arenas": true,
            "go_package": "google.golang.org/genproto/googleapis/type/dayofweek;dayofweek",
            "java_multiple_files": true,
            "java_outer_classname": "DayOfWeekProto",
            "java_package": "com.google.type",
            "objc_class_prefix": "GTP"
          },
          "nested": {
            "Date": {
              "fields": {
                "year": {
                  "type": "int32",
                  "id": 1
                },
                "month": {
                  "type": "int32",
                  "id": 2
                },
                "day": {
                  "type": "int32",
                  "id": 3
                }
              }
            },
            "DateTime": {
              "oneofs": {
                "timeOffset": {
                  "oneof": [
                    "utcOffset",
                    "timeZone"
                  ]
                }
              },
              "fields": {
                "year": {
                  "type": "int32",
                  "id": 1
                },
                "month": {
                  "type": "int32",
                  "id": 2
                },
                "day": {
                  "type": "int32",
                  "id": 3
                },
                "hours": {
                  "type": "int32",
                  "id": 4
                },
                "minutes": {
                  "type": "int32",
                  "id": 5
                },
                "seconds": {
                  "type": "int32",
                  "id": 6
                },
                "nanos": {
                  "type": "int32",
                  "id": 7
                },
                "utcOffset": {
                  "type": "google.protobuf.Duration",
                  "id": 8
                },
                "timeZone": {
                  "type": "TimeZone",
                  "id": 9
                }
              }
            },
            "TimeZone": {
              "fields": {
                "id": {
                  "type": "string",
                  "id": 1
                },
                "version": {
                  "type": "string",
                  "id": 2
                }
              }
            },
            "DayOfWeek": {
              "values": {
                "DAY_OF_WEEK_UNSPECIFIED": 0,
                "MONDAY": 1,
                "TUESDAY": 2,
                "WEDNESDAY": 3,
                "THURSDAY": 4,
                "FRIDAY": 5,
                "SATURDAY": 6,
                "SUNDAY": 7
              }
            }
          }
        }
      }
    }
  }
};
