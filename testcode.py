from pyspark.sql.functions import expr
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    Row,
    IntegerType,
    TimestampType,
    LongType,
    BooleanType,
    FloatType,
)
import tempfile
from typing import Iterator
import pandas as pd

def run_test(spark):
    # FIXME: to simply testing...
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    )
    spark.conf.set("spark.sql.execution.arrow.transformWithStateInPandas.maxRecordsPerBatch", "100")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    checkpoint_path = tempfile.mkdtemp()

    class NoStateInteractionProcessor(StatefulProcessor):
        def init(self, handle: StatefulProcessorHandle) -> None:
            pass

        def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
            for pdf in rows:
                # print(f"before applying change: key {key}, pdf {pdf}")
                pdf["modValue"] = pdf["value"]
                pdf.drop(columns=['value'], inplace=True)
                # print(f"after applying change: key {key}, pdf {pdf}")
                yield pdf

        def close(self) -> None:
            pass


    df = (
        spark.readStream
            .format("rate")
            .option("numPartitions", 1)
            .option("rowsPerSecond", 1000)
            .load()
            .withColumn("groupingKey", expr("uuid()"))
            .drop("timestamp")
    )

    for q in spark.streams.active:
        q.stop()

    output_schema = StructType(
        [
            StructField("groupingKey", StringType(), True),
            StructField("modValue", LongType(), True),
        ]
    )

    q = (
        df.groupBy("groupingKey")
        .transformWithStateInPandas(
            statefulProcessor=NoStateInteractionProcessor(),
            outputStructType=output_schema,
            outputMode="Update",
            timeMode="none",
        )
        .writeStream.queryName("this_query")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 minute")
        .format("console")
        .outputMode("update")
        .start()
    )

