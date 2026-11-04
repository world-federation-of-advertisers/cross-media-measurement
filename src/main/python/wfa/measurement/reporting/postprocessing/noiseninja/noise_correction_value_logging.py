# Copyright 2026 The Cross-Media Measurement Authors
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
"""Controls whether noise-correction measurement values may be logged."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar


_LOG_NOISE_CORRECTION_VALUES: ContextVar[bool] = ContextVar(
    "log_noise_correction_values", default=False
)


def is_enabled() -> bool:
    """Returns whether measurement values may be emitted to application logs."""
    return _LOG_NOISE_CORRECTION_VALUES.get()


@contextmanager
def measurement_value_logging(enabled: bool) -> Iterator[None]:
    """Sets measurement-value logging for the current processing context."""
    token = _LOG_NOISE_CORRECTION_VALUES.set(enabled)
    try:
        yield
    finally:
        _LOG_NOISE_CORRECTION_VALUES.reset(token)
