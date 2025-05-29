from pyspark.sql import functions as F

def clean_date_column(df, col_name):
        """ Function to clean date column for spark"""

        # Fix missing days or months
        temp_df = df.withColumn(
            col_name,
            (
                F
                .when(F.length(col_name) == 4, F.concat(col_name,F.lit("0101")))
                .when(F.length(col_name) == 6, F.concat(col_name,F.lit("01")))
                .otherwise(F.col(col_name))
            )
        )

        # Set date constraints
        temp_df = temp_df.withColumn(
            col_name,
            (
                F
                .when(F.col(col_name) < F.lit("19000101"), None)
                .otherwise(F.col(col_name))
            )
        )

        # Cast to date
        temp_df = temp_df.withColumn(col_name,(F.to_date(col_name,"yyyyMMdd")))

        return temp_df