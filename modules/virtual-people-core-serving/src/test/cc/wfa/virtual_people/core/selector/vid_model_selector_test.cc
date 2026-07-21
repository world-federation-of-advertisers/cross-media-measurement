// Copyright 2023 The Cross-Media Measurement Authors
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

#include "wfa/virtual_people/core/selector/vid_model_selector.h"

#include <string>
#include <vector>

#include "common_cpp/protobuf_util/textproto_io.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"

namespace wfa_virtual_people {
namespace {

using ::wfa::IsOk;
using ::wfa::ReadTextProtoFile;
using ::wfa::StatusIs;

const char kTestDataDir[] = "src/main/resources/testing/selector/";

TEST(VidModelSelectorTest,
     TestBuildVidSelectorObjectWithoutParamsThrowsException) {
  EXPECT_THAT(VidModelSelector::Build(ModelLine{}, std::vector<ModelRollout>{})
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(VidModelSelectorTest,
     TestBuildVidSelectorObjectWithWrongModelRolloutThrowsException) {
  const std::string model_line_path = "model_line_02.textproto";
  const std::string model_rollout_path = "model_rollout_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path),
                                model_rollout),
              IsOk());

  EXPECT_THAT(VidModelSelector::Build(model_line,
                                      std::vector<ModelRollout>{model_rollout})
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(VidModelSelectorTest, TestMissingLabelerIputIdsThrowsException) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path = "model_rollout_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path),
                                model_rollout),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.set_timestamp_usec(1200000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line,
                              std::vector<ModelRollout>{model_rollout}));

  EXPECT_THAT(vid_model_selector.GetModelRelease(labeler_input).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(VidModelSelectorTest, TestReturnsNullWhenModelLineIsNotYetActive) {
  const std::string model_line_path = "model_line_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.set_timestamp_usec(900000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line, std::vector<ModelRollout>{}));

  EXPECT_FALSE(*vid_model_selector.GetModelRelease(labeler_input));
}

TEST(VidModelSelectorTest, TestReturnsNullWhenModelLineIsNoLongerActive) {
  const std::string model_line_path = "model_line_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.set_timestamp_usec(2100000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line, std::vector<ModelRollout>{}));
  EXPECT_FALSE(*vid_model_selector.GetModelRelease(labeler_input));
}

TEST(VidModelSelectorTest, TestReturnsNullWhenModelRolloutsIsEmptyList) {
  const std::string model_line_path = "model_line_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.set_timestamp_usec(1200000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line, std::vector<ModelRollout>{}));
  EXPECT_FALSE(*vid_model_selector.GetModelRelease(labeler_input));
}

TEST(VidModelSelectorTest,
     TestReturnsNullWhenEventTimePrecedesRolloutPeriodStartTime) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path = "model_rollout_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path),
                                model_rollout),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1050000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line,
                              std::vector<ModelRollout>{model_rollout}));
  EXPECT_FALSE(*vid_model_selector.GetModelRelease(labeler_input));
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWhenModelRolloutHasRolloutPeriod) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path = "model_rollout_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path),
                                model_rollout),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1200000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line,
                              std::vector<ModelRollout>{model_rollout}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_01",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWhenModelRolloutHasInstantRollout) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path =
      "model_rollout_without_rollout_period_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path),
                                model_rollout),
              IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1200000000000000LL);

  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(model_line,
                              std::vector<ModelRollout>{model_rollout}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_without_rollout_period_02",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithTwoRolloutsAndEventAfterR2) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1800000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_1, model_rollout_2}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release);
}

TEST(VidModelSelectorTest, TestReturnsModelReleaseWithTwoRolloutsAndEventInR2) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1620000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_2, model_rollout_1}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithTwoRolloutsAndEventSmallerThanR1) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1500000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_2, model_rollout_1}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_01",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithTwoRolloutsAndEventSmallerThanR1AndR2) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "xyz@mail.com");
  labeler_input.set_timestamp_usec(1500000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_1, model_rollout_2}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release);
}

TEST(VidModelSelectorTest, TestReturnsModelReleaseWithTwoRolloutsAndEventInR1) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1200000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_2, model_rollout_1}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_01",
      model_release);
}

TEST(VidModelSelectorTest, TestReturnsSameModelReleaseWithMultipleInvocation) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "xyz@mail.com");
  labeler_input.set_timestamp_usec(1500000000000000LL);

  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_2, model_rollout_1}));
  std::string model_release_1 =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release_1);

  std::string model_release_2 =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release_2);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithThreeRolloutsAndEventSmallerThanR1AndR2) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 = "model_rollout_03.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "xyz@mail.com");
  labeler_input.set_timestamp_usec(1450000000000000LL);
  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(
          model_line, std::vector<ModelRollout>{
                          model_rollout_2, model_rollout_1, model_rollout_3}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithThreeRolloutsAndEventSmallerThanR1AndR3) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 = "model_rollout_03.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "cba@mail.com");
  labeler_input.set_timestamp_usec(1580000000000000LL);
  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(
          model_line, std::vector<ModelRollout>{
                          model_rollout_2, model_rollout_1, model_rollout_3}));
  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_03",
      model_release);
}

TEST(VidModelSelectorTest,
     TestReturnsModelReleaseWithThreeRolloutsAndEventSmallerThanR1) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 = "model_rollout_03.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1450000000000000LL);
  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(
          model_line, std::vector<ModelRollout>{
                          model_rollout_2, model_rollout_1, model_rollout_3}));

  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_01",
      model_release);
}

TEST(VidModelSelectorTest,
     TestExcludeRolloutsPriorToRolloutWithInstantRollout) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 = "model_rollout_03.textproto";
  const std::string model_rollout_path_4 =
      "model_rollout_without_rollout_period_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());
  ModelRollout model_rollout_4;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_4),
                        model_rollout_4),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1450000000000000LL);
  ASSERT_OK_AND_ASSIGN(VidModelSelector vid_model_selector,
                       VidModelSelector::Build(
                           model_line, std::vector<ModelRollout>{
                                           model_rollout_2, model_rollout_1,
                                           model_rollout_3, model_rollout_4}));

  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_without_rollout_period_01",
      model_release);
}

TEST(VidModelSelectorTest, TestBlockRolloutWhenFreezeTimeIsSet) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 =
      "model_rollout_freeze_time_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "xyz@mail.com");
  labeler_input.set_timestamp_usec(1900000000000000LL);
  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(
          model_line, std::vector<ModelRollout>{
                          model_rollout_2, model_rollout_1, model_rollout_3}));

  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_02",
      model_release);
}

TEST(VidModelSelectorTest, TestRolloutWithFreezeTimeIsCorrectlySelected) {
  const std::string model_line_path = "model_line_01.textproto";
  const std::string model_rollout_path_1 = "model_rollout_01.textproto";
  const std::string model_rollout_path_2 = "model_rollout_02.textproto";
  const std::string model_rollout_path_3 =
      "model_rollout_freeze_time_01.textproto";
  ModelLine model_line;
  EXPECT_THAT(ReadTextProtoFile(absl::StrCat(kTestDataDir, model_line_path),
                                model_line),
              IsOk());
  ModelRollout model_rollout_1;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_1),
                        model_rollout_1),
      IsOk());
  ModelRollout model_rollout_2;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_2),
                        model_rollout_2),
      IsOk());
  ModelRollout model_rollout_3;
  EXPECT_THAT(
      ReadTextProtoFile(absl::StrCat(kTestDataDir, model_rollout_path_3),
                        model_rollout_3),
      IsOk());

  LabelerInput labeler_input;
  labeler_input.mutable_profile_info()->mutable_email_user_info()->set_user_id(
      "abc@mail.com");
  labeler_input.set_timestamp_usec(1900000000000000LL);
  ASSERT_OK_AND_ASSIGN(
      VidModelSelector vid_model_selector,
      VidModelSelector::Build(
          model_line, std::vector<ModelRollout>{
                          model_rollout_2, model_rollout_1, model_rollout_3}));

  std::string model_release =
      *vid_model_selector.GetModelRelease(labeler_input).value();

  ASSERT_EQ(
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelReleases/"
      "rollout_freeze_time_01",
      model_release);
}

}  // namespace
}  // namespace wfa_virtual_people
