#ifndef SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_
#define SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_

#define RETURN_IF_ERROR(status)        \
  do {                                 \
    Status _status = (status);         \
    if (!_status.ok()) return _status; \
  } while (0)

#endif  // SRC_MAIN_CC_MEASUREMENT_CRYPTO_UTIL_MACROS_H_
