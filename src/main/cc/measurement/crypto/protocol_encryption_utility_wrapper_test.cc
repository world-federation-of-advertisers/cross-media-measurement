#include "protocol_encryption_utility_wrapper.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace wfa::measurement::crypto {
namespace {

using ::wfa::measurement::internal::duchy::
    BlindLastLayerIndexThenJoinRegistersRequest;
using ::wfa::measurement::internal::duchy::
    BlindLastLayerIndexThenJoinRegistersResponse;
using ::wfa::measurement::internal::duchy::BlindOneLayerRegisterIndexRequest;
using ::wfa::measurement::internal::duchy::BlindOneLayerRegisterIndexResponse;
using ::wfa::measurement::internal::duchy::DecryptLastLayerFlagAndCountRequest;
using ::wfa::measurement::internal::duchy::DecryptLastLayerFlagAndCountResponse;
using ::wfa::measurement::internal::duchy::DecryptOneLayerFlagAndCountRequest;
using ::wfa::measurement::internal::duchy::DecryptOneLayerFlagAndCountResponse;

TEST(BlindOneLayerRegisterIndex, validEncrypterShouldEncryptSuccessfully) {
  BlindOneLayerRegisterIndexRequest request_proto;
  private_join_and_compute::StatusOr<std::string> result =
      BlindOneLayerRegisterIndex(request_proto.SerializeAsString());
  ASSERT_TRUE(!result.ok());
  EXPECT_NE(result.status().message().find("unimplemented"), std::string::npos);
}

TEST(BlindLastLayerIndexThenJoinRegisters,
     validEncrypterShouldEncryptSuccessfully) {
  BlindLastLayerIndexThenJoinRegistersRequest request_proto;
  private_join_and_compute::StatusOr<std::string> result =
      BlindLastLayerIndexThenJoinRegisters(request_proto.SerializeAsString());
  ASSERT_TRUE(!result.ok());
  EXPECT_NE(result.status().message().find("unimplemented"), std::string::npos);
}

TEST(DecryptOneLayerFlagAndCount, validEncrypterShouldEncryptSuccessfully) {
  DecryptOneLayerFlagAndCountRequest request_proto;
  private_join_and_compute::StatusOr<std::string> result =
      DecryptOneLayerFlagAndCount(request_proto.SerializeAsString());
  ASSERT_TRUE(!result.ok());
  EXPECT_NE(result.status().message().find("unimplemented"), std::string::npos);
}

TEST(DecryptLastLayerFlagAndCount, validEncrypterShouldEncryptSuccessfully) {
  DecryptLastLayerFlagAndCountRequest request_proto;
  private_join_and_compute::StatusOr<std::string> result =
      DecryptLastLayerFlagAndCount(request_proto.SerializeAsString());
  ASSERT_TRUE(!result.ok());
  EXPECT_NE(result.status().message().find("unimplemented"), std::string::npos);
}

}  // namespace
}  // namespace wfa::measurement::crypto
