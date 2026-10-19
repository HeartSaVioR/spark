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

import json
from collections.abc import Iterator as IteratorABC
from collections.abc import Sequence as SequenceABC
from typing import IO, Iterable, List

import pyarrow as pa

from pyspark.errors import PySparkAssertionError, PySparkRuntimeError, PySparkTypeError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import (
    SpecialLengths,
    read_bool,
    read_int,
    write_int,
)
from pyspark.sql.datasource import (
    DataSource,
    EqualNullSafe,
    EqualTo,
    ExternalLookup,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    LookupParameter,
)
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import (
    DataType,
    IntegerType,
    StructField,
    StructType,
    _parse_datatype_json_string,
)
from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches
from pyspark.sql.worker.utils import worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
    utf8_deserializer,
)

INPUT_ORDINAL_COLUMN_NAME = "input_ordinal"


_LOOKUP_PREDICATE_TYPES = {
    "=": EqualTo,
    "<=>": EqualNullSafe,
    ">": GreaterThan,
    ">=": GreaterThanOrEqual,
    "<": LessThan,
    "<=": LessThanOrEqual,
}


def deserialize_lookup_predicate(json_dict: dict) -> Filter:
    data_type_json = json_dict["parameterDataType"]
    data_type = _parse_datatype_json_string(data_type_json)
    if not isinstance(data_type, DataType):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a lookup parameter data type of type 'DataType'",
                "actual": f"'{type(data_type).__name__}'",
            },
        )
    predicate_type = _LOOKUP_PREDICATE_TYPES.get(json_dict["name"])
    if predicate_type is None:
        raise PySparkTypeError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a supported external lookup predicate",
                "actual": str(json_dict["name"]),
            },
        )
    return predicate_type(
        attribute=tuple(json_dict["field"]),
        value=LookupParameter(dataType=data_type),
    )


def validate_selected_predicate_indexes(
    lookup: ExternalLookup, predicates: List[Filter]
) -> List[int]:
    selected = lookup.selectedPredicateIndexes()
    if not isinstance(selected, SequenceABC) or isinstance(selected, (str, bytes, bytearray)):
        raise PySparkTypeError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "'selectedPredicateIndexes' to return a sequence",
                "actual": f"'{type(selected).__name__}'",
            },
        )

    selected_indexes = list(selected)
    for index in selected_indexes:
        if not isinstance(index, int) or isinstance(index, bool):
            raise PySparkTypeError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "selected predicate indexes to be integers",
                    "actual": f"'{type(index).__name__}'",
                },
            )
        if index < 0 or index >= len(predicates):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": f"selected predicate indexes in range [0, {len(predicates)})",
                    "actual": str(index),
                },
            )

    if len(set(selected_indexes)) != len(selected_indexes):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "distinct selected predicate indexes",
                "actual": str(selected_indexes),
            },
        )
    return selected_indexes


def validate_max_parameter_rows_per_request(lookup: ExternalLookup) -> int:
    max_parameter_rows = lookup.maxParameterRowsPerRequest()
    if not isinstance(max_parameter_rows, int) or isinstance(max_parameter_rows, bool):
        raise PySparkTypeError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "'maxParameterRowsPerRequest' to return an integer",
                "actual": f"'{type(max_parameter_rows).__name__}'",
            },
        )
    if max_parameter_rows <= 0 or max_parameter_rows > 2**31 - 1:
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "'maxParameterRowsPerRequest' in range [1, 2147483647]",
                "actual": str(max_parameter_rows),
            },
        )
    return max_parameter_rows


def lookup_output_schema(output_schema: StructType) -> StructType:
    return StructType(
        [StructField(INPUT_ORDINAL_COLUMN_NAME, IntegerType(), False)] + list(output_schema.fields)
    )


def _main(infile: IO, outfile: IO) -> None:
    """
    Plan a Python data source external lookup.

    This worker chooses one lookup access path from Spark-provided candidates and
    returns a pickled Arrow function that performs batched lookup at execution.
    """
    data_source = read_command(pickleSer, infile)
    if not isinstance(data_source, DataSource):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a Python data source instance of type 'DataSource'",
                "actual": f"'{type(data_source).__name__}'",
            },
        )

    schema_json = utf8_deserializer.loads(infile)
    schema = _parse_datatype_json_string(schema_json)
    if not isinstance(schema, StructType):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an output schema of type 'StructType'",
                "actual": f"'{type(schema).__name__}'",
            },
        )

    predicate_dicts = json.loads(utf8_deserializer.loads(infile))
    predicates = [deserialize_lookup_predicate(predicate) for predicate in predicate_dicts]

    max_arrow_batch_size = read_int(infile)
    assert max_arrow_batch_size > 0, (
        f"The maximum arrow batch size should be greater than 0, but got '{max_arrow_batch_size}'"
    )
    read_bool(infile)

    with capture_outputs():
        lookup = data_source.externalLookup(schema=schema, predicates=predicates)
        if lookup is None:
            write_int(SpecialLengths.NULL, outfile)
            return
        if not isinstance(lookup, ExternalLookup):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of ExternalLookup",
                    "actual": f"'{type(lookup).__name__}'",
                },
            )

        selected_predicate_indexes = validate_selected_predicate_indexes(lookup, predicates)
        max_parameter_rows = validate_max_parameter_rows_per_request(lookup)
        return_type = lookup_output_schema(schema)
        arrow_schema = to_arrow_schema(return_type, timezone="UTC")

        def data_source_lookup_func(iterator: Iterable[pa.RecordBatch]) -> Iterable[pa.RecordBatch]:
            output_iter = lookup.lookup(iterator)
            if not isinstance(output_iter, IteratorABC):
                raise PySparkRuntimeError(
                    errorClass="DATA_SOURCE_INVALID_RETURN_TYPE",
                    messageParameters={
                        "type": type(output_iter).__name__,
                        "name": data_source.name(),
                        "supported_types": "iterator",
                    },
                )
            try:
                for batch in records_to_arrow_batches(
                    output_iter, max_arrow_batch_size, return_type, data_source
                ):
                    if not arrow_schema.equals(batch.schema):
                        raise PySparkRuntimeError(
                            errorClass="DATA_SOURCE_RETURN_SCHEMA_MISMATCH",
                            messageParameters={
                                "expected": str(arrow_schema),
                                "actual": str(batch.schema),
                            },
                        )
                    if batch.column(0).null_count:
                        raise PySparkRuntimeError(
                            errorClass="DATA_SOURCE_TYPE_MISMATCH",
                            messageParameters={
                                "expected": "non-null input_ordinal values",
                                "actual": "null input_ordinal",
                            },
                        )
                    yield batch
            finally:
                close = getattr(output_iter, "close", None)
                if close is not None:
                    close()

        data_source_lookup_func.__module__ = "pyspark.sql.worker.plan_data_source_lookup"

        command = (data_source_lookup_func, return_type)
        pickleSer._write_with_length(command, outfile)
        write_int(max_parameter_rows, outfile)
        write_int(len(selected_predicate_indexes), outfile)
        for index in selected_predicate_indexes:
            write_int(index, outfile)


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
