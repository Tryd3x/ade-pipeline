from pyspark.sql import functions as F
from pyspark.sql.types import StringType

class Reaction:
    def __init__(self, df):
        self.df = df
        
    def get_df(self):
        return self.df
    
    def cast(self):
        self.df = (
            self.df
            .withColumn("patientid", F.col("patientid").cast(StringType()))
            .withColumn("reactionmeddrapt", F.col("reactionmeddrapt").cast(StringType()))
            .withColumn("reactionoutcome", F.col("reactionoutcome").cast(StringType()))
        )

    def transform(self):
        self.df = (
            self.df
            .withColumn(
                "reactionoutcome",
                (
                    F
                    .when(F.col("reactionoutcome") == '1', "Recovered/resolved")
                    .when(F.col("reactionoutcome") == '2', "Recovering/resolving")
                    .when(F.col("reactionoutcome") == '3', "Not recovered/not resolved")
                    .when(F.col("reactionoutcome") == '4', "Recovered/resolved with sequelae (consequent health issues)")
                    .when(F.col("reactionoutcome") == '5', "Fatal")
                    .when(F.col("reactionoutcome") == '6', "Unknown")
                    .otherwise(None)
                ).cast(StringType())
            )
        )