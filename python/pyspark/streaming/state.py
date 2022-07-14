#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import datetime

from pyspark.sql.types import DateType

__all__ = ["GroupStateImpl", "GroupStateTimeout"]


class GroupStateTimeout:
    @staticmethod
    def from_string(clazz_name):
        if clazz_name == "NoTimeout":
            return NoTimeout
        elif clazz_name == "ProcessingTimeTimeout":
            return ProcessingTimeTimeout
        elif clazz_name == "EventTimeTimeout":
            return EventTimeTimeout

    @staticmethod
    def ProcessingTimeTimeout():
        return ProcessingTimeTimeout

    @staticmethod
    def EventTimeTimeout():
        return EventTimeTimeout

    @staticmethod
    def NoTimeout():
        return NoTimeout


class NoTimeout(GroupStateTimeout):
    _clazz_name = "NoTimeout"


class ProcessingTimeTimeout(GroupStateTimeout):
    _clazz_name = "ProcessingTimeTimeout"


class EventTimeTimeout(GroupStateTimeout):
    _clazz_name = "EventTimeTimeout"


class GroupStateImpl:
    NO_TIMESTAMP = -1

    def __init__(
        self,
        optionalValue,
        batchProcessingTimeMs,
        eventTimeWatermarkMs,
        timeoutConf,
        hasTimedOut,
        watermarkPresent,
    ) -> None:
        self._value = optionalValue
        self._batch_processing_time_ms = batchProcessingTimeMs
        self._event_time_watermark_ms = eventTimeWatermarkMs
        self._timeout_conf = timeoutConf
        self._has_timed_out = hasTimedOut
        self._watermark_present = watermarkPresent

        self._serde_able_properties = {
            "batchProcessingTimeMs": self._batch_processing_time_ms,
            "eventTimeWatermarkMs": self._event_time_watermark_ms,
            "hasTimedOut": self._has_timed_out,
            "watermarkPresent": self._watermark_present,
        }

        self._defined = self._value is not None
        self._updated = False
        self._removed = False
        self._timeout_timestamp = GroupStateImpl.NO_TIMESTAMP

    @property
    def exists(self):
        return self._defined

    @property
    def get(self):
        if self.exists:
            return self._value
        else:
            raise ValueError("State is either not defined or has already been removed")

    @property
    def getOption(self):
        return self._value

    def update(self, newValue):
        if newValue is None:
            raise ValueError("'None' is not a valid state value")

        self._value = newValue
        self._defined = True
        self._updated = True
        self._removed = False

    def remove(self):
        self._defined = False
        self._updated = False
        self._removed = True

    def setTimeoutDuration(self, durationMs):
        if isinstance(durationMs, str):
            # FIXME: should copy the parse logic.
            raise Exception("Not implemented yet")

        if self._timeout_conf != ProcessingTimeTimeout:
            raise RuntimeError(
                "Cannot set timeout duration without enabling processing time timeout in "
                "[map|flatMap]GroupsWithState"
            )

        if durationMs <= 0:
            raise ValueError("Timeout duration must be positive")
        self._timeout_timestamp = durationMs + self._batch_processing_time_ms

    # FIXME: implement additionalDuration
    def setTimeoutTimestamp(self, timestampMs):
        self._checkTimeoutTimestampAllowed()
        if isinstance(timestampMs, datetime.datetime):
            timestampMs = DateType().toInternal(timestampMs)

        if timestampMs <= 0:
            raise ValueError("Timeout timestamp must be positive")

        if (
            self._event_time_watermark_ms != GroupStateImpl.NO_TIMESTAMP
            and timestampMs < self._event_time_watermark_ms
        ):
            raise ValueError(
                "Timeout timestamp (%s) cannot be earlier than the "
                "current watermark (%s)" % (timestampMs, self._event_time_watermark_ms)
            )

        self._timeout_timestamp = timestampMs

    def getCurrentWatermarkMs(self):
        if not self._watermark_present:
            raise RuntimeError(
                "Cannot get event time watermark timestamp without setting watermark before "
                "[map|flatMap]GroupsWithState"
            )
        return self._event_time_watermark_ms

    def getCurrentProcessingTimeMs(self):
        return self._batch_processing_time_ms

    def __str__(self):
        if self.exists:
            return "GroupState(%s)" % self.get
        else:
            return "GroupState(<undefined>)})"

    def json(self):
        return ".."

    def _checkTimeoutTimestampAllowed(self):
        if self._timeout_conf is EventTimeTimeout:
            raise RuntimeError(
                "Cannot set timeout duration without enabling processing time timeout in "
                "[map|flatMap]GroupsWithState"
            )
