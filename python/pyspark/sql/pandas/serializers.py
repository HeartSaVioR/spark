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

"""
Serializers for PyArrow and pandas conversions. See `pyspark.serializers` for more details.
"""
import sys
import time

from pyspark.serializers import Serializer, read_int, write_int, UTF8Deserializer, CPickleSerializer
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import StringType, StructType, BinaryType, StructField, BooleanType


class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6
    START_STATE_UPDATE = -7


class ArrowCollectSerializer(Serializer):
    """
    Deserialize a stream of batches followed by batch order information. Used in
    PandasConversionMixin._collect_as_arrow() after invoking Dataset.collectAsArrowToPython()
    in the JVM.
    """

    def __init__(self):
        self.serializer = ArrowStreamSerializer()

    def dump_stream(self, iterator, stream):
        return self.serializer.dump_stream(iterator, stream)

    def load_stream(self, stream):
        """
        Load a stream of un-ordered Arrow RecordBatches, where the last iteration yields
        a list of indices that can be used to put the RecordBatches in the correct order.
        """
        # load the batches
        for batch in self.serializer.load_stream(stream):
            yield batch

        # load the batch order indices or propagate any error that occurred in the JVM
        num = read_int(stream)
        if num == -1:
            error_msg = UTF8Deserializer().loads(stream)
            raise RuntimeError(
                "An error occurred while calling "
                "ArrowCollectSerializer.load_stream: {}".format(error_msg)
            )
        batch_order = []
        for i in range(num):
            index = read_int(stream)
            batch_order.append(index)
        yield batch_order

    def __repr__(self):
        return "ArrowCollectSerializer(%s)" % self.serializer


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a stream.
    """

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream):
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def __repr__(self):
        return "ArrowStreamSerializer"


class ArrowStreamUDFSerializer(ArrowStreamSerializer):
    """
    Same as :class:`ArrowStreamSerializer` but it flattens the struct to Arrow record batch
    for applying each function with the raw record arrow batch. See also `DataFrame.mapInArrow`.
    """

    def load_stream(self, stream):
        """
        Flatten the struct into Arrow's record batches.
        """
        import pyarrow as pa

        batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
        for batch in batches:
            struct = batch.column(0)
            yield [pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))]

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def wrap_and_init_stream():
            should_write_start_length = True
            for batch, _ in iterator:
                assert isinstance(batch, pa.RecordBatch)

                # Wrap the root struct
                struct = pa.StructArray.from_arrays(
                    batch.columns, fields=pa.struct(list(batch.schema))
                )
                batch = pa.RecordBatch.from_arrays([struct], ["_0"])

                # Write the first record batch with initialization.
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return super(ArrowStreamUDFSerializer, self).dump_stream(wrap_and_init_stream(), stream)


class ArrowStreamPandasSerializer(ArrowStreamSerializer):
    """
    Serializes Pandas.Series as Arrow data with Arrow streaming format.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    """

    def __init__(self, timezone, safecheck, assign_cols_by_name):
        super(ArrowStreamPandasSerializer, self).__init__()
        self._timezone = timezone
        self._safecheck = safecheck
        self._assign_cols_by_name = assign_cols_by_name

    def arrow_to_pandas(self, arrow_column):
        from pyspark.sql.pandas.types import (
            _check_series_localize_timestamps,
            _convert_map_items_to_dict,
        )
        import pyarrow

        # If the given column is a date type column, creates a series of datetime.date directly
        # instead of creating datetime64[ns] as intermediate data to avoid overflow caused by
        # datetime64[ns] type handling.
        s = arrow_column.to_pandas(date_as_object=True)

        if pyarrow.types.is_timestamp(arrow_column.type) and arrow_column.type.tz is not None:
            return _check_series_localize_timestamps(s, self._timezone)
        elif pyarrow.types.is_map(arrow_column.type):
            return _convert_map_items_to_dict(s)
        else:
            return s

    def _create_batch(self, series):
        """
        Create an Arrow record batch from the given pandas.Series or list of Series,
        with optional type.

        Parameters
        ----------
        series : pandas.Series or list
            A single series, list of series, or list of (series, arrow_type)

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pandas as pd
        import pyarrow as pa
        from pyspark.sql.pandas.types import (
            _check_series_convert_timestamps_internal,
            _convert_dict_to_map_items,
        )
        from pandas.api.types import is_categorical_dtype

        # Make input conform to [(series1, type1), (series2, type2), ...]
        if not isinstance(series, (list, tuple)) or (
            len(series) == 2 and isinstance(series[1], pa.DataType)
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)

        def create_array(s, t):
            if hasattr(s.array, "__arrow_array__"):
                mask = None
            else:
                mask = s.isnull()
            # Ensure timestamp series are in expected form for Spark internal representation
            if t is not None and pa.types.is_timestamp(t) and t.tz is not None:
                s = _check_series_convert_timestamps_internal(s, self._timezone)
            elif t is not None and pa.types.is_map(t):
                s = _convert_dict_to_map_items(s)
            elif is_categorical_dtype(s.dtype):
                # Note: This can be removed once minimum pyarrow version is >= 0.16.1
                s = s.astype(s.dtypes.categories.dtype)
            try:
                array = pa.Array.from_pandas(s, mask=mask, type=t, safe=self._safecheck)
            except ValueError as e:
                if self._safecheck:
                    error_msg = (
                        "Exception thrown when converting pandas.Series (%s) to "
                        + "Arrow Array (%s). It can be caused by overflows or other "
                        + "unsafe conversions warned by Arrow. Arrow safe type check "
                        + "can be disabled by using SQL config "
                        + "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                    )
                    raise ValueError(error_msg % (s.dtype, t)) from e
                else:
                    raise e
            return array

        arrs = []
        for s, t in series:
            if t is not None and pa.types.is_struct(t):
                if not isinstance(s, pd.DataFrame):
                    raise ValueError(
                        "A field of type StructType expects a pandas.DataFrame, "
                        "but got: %s" % str(type(s))
                    )

                # Input partition and result pandas.DataFrame empty, make empty Arrays with struct
                if len(s) == 0 and len(s.columns) == 0:
                    arrs_names = [(pa.array([], type=field.type), field.name) for field in t]
                # Assign result columns by schema name if user labeled with strings
                elif self._assign_cols_by_name and any(isinstance(name, str) for name in s.columns):
                    arrs_names = [
                        (create_array(s[field.name], field.type), field.name) for field in t
                    ]
                # Assign result columns by  position
                else:
                    arrs_names = [
                        (create_array(s[s.columns[i]], field.type), field.name)
                        for i, field in enumerate(t)
                    ]

                struct_arrs, struct_names = zip(*arrs_names)
                arrs.append(pa.StructArray.from_arrays(struct_arrs, struct_names))
            else:
                arrs.append(create_array(s, t))

        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def dump_stream(self, iterator, stream):
        """
        Make ArrowRecordBatches from Pandas Series and serialize. Input is a single series or
        a list of series accompanied by an optional pyarrow type to coerce the data to.
        """
        batches = (self._create_batch(series) for series in iterator)
        super(ArrowStreamPandasSerializer, self).dump_stream(batches, stream)

    def load_stream(self, stream):
        """
        Deserialize ArrowRecordBatches to an Arrow table and return as a list of pandas.Series.
        """
        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        import pyarrow as pa

        for batch in batches:
            yield [self.arrow_to_pandas(c) for c in pa.Table.from_batches([batch]).itercolumns()]

    def __repr__(self):
        return "ArrowStreamPandasSerializer"


class ArrowStreamPandasUDFSerializer(ArrowStreamPandasSerializer):
    """
    Serializer used by Python worker to evaluate Pandas UDFs
    """

    def __init__(self, timezone, safecheck, assign_cols_by_name, df_for_struct=False):
        super(ArrowStreamPandasUDFSerializer, self).__init__(
            timezone, safecheck, assign_cols_by_name
        )
        self._df_for_struct = df_for_struct

    def arrow_to_pandas(self, arrow_column):
        import pyarrow.types as types

        if self._df_for_struct and types.is_struct(arrow_column.type):
            import pandas as pd

            series = [
                super(ArrowStreamPandasUDFSerializer, self)
                .arrow_to_pandas(column)
                .rename(field.name)
                for column, field in zip(arrow_column.flatten(), arrow_column.type)
            ]
            s = pd.concat(series, axis=1)
        else:
            s = super(ArrowStreamPandasUDFSerializer, self).arrow_to_pandas(arrow_column)
        return s

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """

        def init_stream_yield_batches():
            should_write_start_length = True
            for series in iterator:
                batch = self._create_batch(series)
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return ArrowStreamSerializer.dump_stream(self, init_stream_yield_batches(), stream)

    def __repr__(self):
        return "ArrowStreamPandasUDFSerializer"


class CogroupUDFSerializer(ArrowStreamPandasUDFSerializer):
    def load_stream(self, stream):
        """
        Deserialize Cogrouped ArrowRecordBatches to a tuple of Arrow tables and yield as two
        lists of pandas.Series.
        """
        import pyarrow as pa

        dataframes_in_group = None

        while dataframes_in_group is None or dataframes_in_group > 0:
            dataframes_in_group = read_int(stream)

            if dataframes_in_group == 2:
                batch1 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                batch2 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                yield (
                    [self.arrow_to_pandas(c) for c in pa.Table.from_batches(batch1).itercolumns()],
                    [self.arrow_to_pandas(c) for c in pa.Table.from_batches(batch2).itercolumns()],
                )

            elif dataframes_in_group != 0:
                raise ValueError(
                    "Invalid number of pandas.DataFrames in group {0}".format(dataframes_in_group)
                )


class ApplyInPandasWithStateSerializer(ArrowStreamPandasUDFSerializer):

    def __init__(self, timezone, safecheck, assign_cols_by_name, state_object_schema,
                 softLimitBytesPerBatch, minDataCountForSample, softTimeoutMillisPurgeBatch):
        super(ApplyInPandasWithStateSerializer, self).__init__(
            timezone, safecheck, assign_cols_by_name)
        self.pickleSer = CPickleSerializer()
        self.utf8_deserializer = UTF8Deserializer()
        self.state_object_schema = state_object_schema

        self.result_state_df_type = StructType([
            StructField('properties', StringType()),
            StructField('keyRowAsUnsafe', BinaryType()),
            StructField('object', BinaryType()),
        ])

        self.result_state_pdf_arrow_type = to_arrow_type(self.result_state_df_type)
        self.softLimitBytesPerBatch = softLimitBytesPerBatch
        self.minDataCountForSample = minDataCountForSample
        self.softTimeoutMillisPurgeBatch = softTimeoutMillisPurgeBatch

    def load_stream(self, stream):
        import pyarrow as pa
        import json
        from itertools import groupby
        from pyspark.sql.streaming.state import GroupStateImpl

        def gen_data_and_state(batches):
            state_for_current_group = None

            for batch in batches:
                batch_schema = batch.schema
                data_schema = pa.schema([batch_schema[i] for i in range(0, len(batch_schema) - 1)])
                state_schema = pa.schema([batch_schema[-1], ])

                batch_columns = batch.columns
                data_columns = batch_columns[0:-1]
                state_column = batch_columns[-1]

                data_batch = pa.RecordBatch.from_arrays(data_columns, schema=data_schema)
                state_batch = pa.RecordBatch.from_arrays([state_column, ], schema=state_schema)

                state_arrow = pa.Table.from_batches([state_batch]).itercolumns()
                state_pandas = [self.arrow_to_pandas(c) for c in state_arrow][0]

                for state_idx in range(0, len(state_pandas)):
                    state_info_col = state_pandas.iloc[state_idx]

                    if not state_info_col:
                        # no more data with grouping key + state
                        break

                    state_info_col_properties = state_info_col['properties']
                    state_info_col_key_row = state_info_col['keyRowAsUnsafe']
                    state_info_col_object = state_info_col['object']

                    data_start_offset = state_info_col['startOffset']
                    num_data_rows = state_info_col['numRows']
                    is_last_chunk = state_info_col['isLastChunk']

                    state_properties = json.loads(state_info_col_properties)
                    if state_info_col_object:
                        state_object = self.pickleSer.loads(state_info_col_object)
                    else:
                        state_object = None
                    state_properties["optionalValue"] = state_object

                    if state_for_current_group:
                        # use the state, we already have state for same group and there should be some
                        # data in same group being processed earlier
                        state = state_for_current_group
                    else:
                        # there is no state being stored for same group, construct one
                        state = GroupStateImpl(keyAsUnsafe=state_info_col_key_row,
                                               valueSchema=self.state_object_schema,
                                               **state_properties)

                    if is_last_chunk:
                        # discard the state being cached for same group
                        state_for_current_group = None
                    elif not state_for_current_group:
                        # there's no cached state but expected to have additional data in same group
                        # cache the current state
                        state_for_current_group = state

                    data_batch_for_group = data_batch.slice(data_start_offset, num_data_rows)
                    data_arrow = pa.Table.from_batches([data_batch_for_group]).itercolumns()

                    data_pandas = [self.arrow_to_pandas(c) for c in data_arrow]

                    # state info
                    yield (data_pandas, state, )

        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)

        data_state_generator = gen_data_and_state(batches)

        for state, data in groupby(data_state_generator, key=lambda x: x[1]):
            yield (data, state,)

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """

        def construct_record_batch(pdfs, pdf_data_cnt, pdf_schema, state_pdfs, state_data_cnt):
            import pandas as pd
            import pyarrow as pa

            max_data_cnt = max(pdf_data_cnt, state_data_cnt)

            empty_row_cnt_in_data = max_data_cnt - pdf_data_cnt
            empty_row_cnt_in_state = max_data_cnt - state_data_cnt

            empty_rows_pdf = pd.DataFrame(
                    dict.fromkeys(pa.schema(pdf_schema).names),
                    index=[x for x in range(0, empty_row_cnt_in_data)])
            empty_rows_state = pd.DataFrame(
                    columns=['properties', 'keyRowAsUnsafe', 'object'],
                    index=[x for x in range(0, empty_row_cnt_in_state)])

            pdfs.append(empty_rows_pdf)
            state_pdfs.append(empty_rows_state)

            merged_pdf = pd.concat(pdfs, ignore_index=True)
            merged_state_pdf = pd.concat(state_pdfs, ignore_index=True)

            return self._create_batch([
                    (merged_pdf, pdf_schema),
                    (merged_state_pdf, self.result_state_pdf_arrow_type)])

        def init_stream_yield_batches():
            import pandas as pd
            import pyarrow as pa

            should_write_start_length = True

            pdfs = []
            state_pdfs = []
            return_schema = None

            pdf_data_cnt = 0
            state_data_cnt = 0

            sampled_data_size_per_row = 0
            sampled_state_size = 0
            # FIXME: sample with empty state size separately?
            sampled_empty_state_size = 0

            last_purged_time_ns = time.time_ns()

            for data in iterator:
                packaged_result = data[0]

                pdf = packaged_result[0][0]
                state = packaged_result[0][1]
                # this won't change across batches
                return_schema = packaged_result[1]

                # FIXME: arrow type to pandas type
                # FIXME: probably also need to check columns to validate?

                if len(pdf) > 0:
                    pdf_data_cnt += len(pdf)
                    pdfs.append(pdf)

                # pick up state for only last chunk as state should have been updated so far
                state_properties = state.json().encode("utf-8")
                state_key_row_as_binary = state._keyAsUnsafe
                state_object = self.pickleSer.dumps(state._value_schema.toInternal(state._value))

                state_dict = {
                    'properties': [state_properties, ],
                    'keyRowAsUnsafe': [state_key_row_as_binary, ],
                    'object': [state_object, ],
                }

                state_pdf = pd.DataFrame.from_dict(state_dict)

                state_pdfs.append(state_pdf)
                state_data_cnt += 1

                # FIXME: threshold of sample data
                if sampled_data_size_per_row == 0 and pdf_data_cnt > self.minDataCountForSample:
                    memory_usages = [p.memory_usage(deep=True).sum() for p in pdfs]
                    sampled_data_size_per_row = sum(memory_usages) / pdf_data_cnt

                # FIXME: threshold of sample data
                if sampled_state_size == 0 and state_data_cnt > self.minDataCountForSample:
                    memory_usages = [p.memory_usage(deep=True).sum() for p in state_pdfs]
                    sampled_state_size = sum(memory_usages) / state_data_cnt

                # This effectively works after the sampling has completed, size we multiply by 0
                # if the sampling is still in progress.
                batch_over_limit_on_size = (sampled_data_size_per_row * pdf_data_cnt) + \
                    (sampled_state_size * state_data_cnt) >= self.softLimitBytesPerBatch
                cur_time_ns = time.time_ns()
                is_timed_out_on_purge = ((cur_time_ns - last_purged_time_ns) // 1000000) >= \
                    self.softTimeoutMillisPurgeBatch
                if batch_over_limit_on_size or is_timed_out_on_purge:
                    batch = construct_record_batch(pdfs, pdf_data_cnt, return_schema,
                                                   state_pdfs, state_data_cnt)

                    pdfs = []
                    state_pdfs = []
                    pdf_data_cnt = 0
                    state_data_cnt = 0
                    last_purged_time_ns = cur_time_ns

                    if should_write_start_length:
                        write_int(SpecialLengths.START_ARROW_STREAM, stream)
                        should_write_start_length = False

                    yield batch

            # end of loop, we may have remaining data
            if pdf_data_cnt > 0 or state_data_cnt > 0:
                batch = construct_record_batch(pdfs, pdf_data_cnt, return_schema,
                                               state_pdfs, state_data_cnt)

                pdfs = []
                state_pdfs = []
                pdf_data_cnt = 0
                state_data_cnt = 0

                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False

                yield batch

        return ArrowStreamSerializer.dump_stream(self, init_stream_yield_batches(), stream)
