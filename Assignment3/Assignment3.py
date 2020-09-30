import sys

from pyspark import keyword_only
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.param.shared import HasInputCols, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum


def main():
    # Setup Spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # put in sql database username here
    username = ""
    # put in sql database password here
    pword = ""

    # load in the batter_counts table from the baseball database.
    # Works with default port of 3306. Change url if needed
    battercounts = (
        spark.read.format("jdbc")
        .options(
            url="jdbc:mysql://localhost:3306/baseball",
            driver="com.mysql.cj.jdbc.Driver",
            user=username,
            password=pword,
            query="SELECT game_id, batter, Hit, atBat FROM batter_counts WHERE atBat>0",
        )
        .load()
    )

    # load in the game table from the baseball database.
    # Works with default port of 3306. Change url if needed
    game = (
        spark.read.format("jdbc")
        .options(
            url="jdbc:mysql://localhost:3306/baseball",
            driver="com.mysql.cj.jdbc.Driver",
            user=username,
            password=pword,
            query="SELECT game_id, local_date as gamedate FROM game",
        )
        .load()
    )

    battercounts.show()

    game.show()

    batter_join = battercounts.join(game, on=["game_id"], how="left")

    # create a transformer that creates a rolling hits variable
    class getrollinghitsTransform(
        Transformer,
        HasInputCols,
        HasOutputCol,
        DefaultParamsReadable,
        DefaultParamsWritable,
    ):
        @keyword_only
        def __init__(self, inputCols=None, outputCol=None):
            super(getrollinghitsTransform, self).__init__()
            kwargs = self._input_kwargs
            self.setParams(**kwargs)
            return

        @keyword_only
        def setParams(self, inputCols=None, outputCol=None):
            kwargs = self._input_kwargs
            return self._set(**kwargs)

        def _transform(self, dataset):
            input_cols = self.getInputCols()
            output_col = self.getOutputCol()

            def days(i):
                return i * 86400

            def hours(i):
                return i * 3600

            partition = (
                Window.partitionBy("batter")
                .orderBy(col("gamedate").cast("timestamp").cast("long"))
                .rangeBetween(days(-100), hours(-6))
            )

            dataset = dataset.withColumn(output_col, sum(input_cols[1]).over(partition))

            return dataset

    # create a transformer that creates a rolling at bats variable
    class getrollingatbatsTransform(
        Transformer,
        HasInputCols,
        HasOutputCol,
        DefaultParamsReadable,
        DefaultParamsWritable,
    ):
        @keyword_only
        def __init__(self, inputCols=None, outputCol=None):
            super(getrollingatbatsTransform, self).__init__()
            kwargs = self._input_kwargs
            self.setParams(**kwargs)
            return

        @keyword_only
        def setParams(self, inputCols=None, outputCol=None):
            kwargs = self._input_kwargs
            return self._set(**kwargs)

        def _transform(self, dataset):
            input_cols = self.getInputCols()
            output_col = self.getOutputCol()

            def days(i):
                return i * 86400

            def hours(i):
                return i * 3600

            partition = (
                Window.partitionBy("batter")
                .orderBy(col("gamedate").cast("timestamp").cast("long"))
                .rangeBetween(days(-100), hours(-6))
            )

            dataset = dataset.withColumn(output_col, sum(input_cols[1]).over(partition))

            return dataset

    # create a transformer that creates a rolling battingaverage variable
    class getrollingbattingaverageTransform(
        Transformer,
        HasInputCols,
        HasOutputCol,
        DefaultParamsReadable,
        DefaultParamsWritable,
    ):
        @keyword_only
        def __init__(self, inputCols=None, outputCol=None):
            super(getrollingbattingaverageTransform, self).__init__()
            kwargs = self._input_kwargs
            self.setParams(**kwargs)
            return

        @keyword_only
        def setParams(self, inputCols=None, outputCol=None):
            kwargs = self._input_kwargs
            return self._set(**kwargs)

        def _transform(self, dataset):
            output_col = self.getOutputCol()

            dataset = dataset.withColumn(
                output_col, col("rollinghits") / col("rollingatbats")
            )

            return dataset

    # Run all three transformers to create new RBA table
    pipeline = Pipeline(
        stages=[
            getrollinghitsTransform(
                inputCols=["batter", "Hit", "gamedate"], outputCol="rollinghits"
            ),
            getrollingatbatsTransform(
                inputCols=["batter", "atBat", "gamedate"], outputCol="rollingatbats"
            ),
            getrollingbattingaverageTransform(
                inputCols=["rollinghits", "rollingatbats"],
                outputCol="RollingBattingAverage",
            ),
        ]
    )

    model = pipeline.fit(batter_join)
    RBA = model.transform(batter_join)
    RBA.show()


if __name__ == "__main__":
    sys.exit(main())
