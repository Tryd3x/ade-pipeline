from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType

class Patient:
    def __init__(self, df):
        self.df = df
    
    def get_df(self):
        return self.df

    def cast(self):
        self.df = (
            self.df
            .withColumn("patientid", F.col("patientid").cast(StringType()))
            .withColumn("patientagegroup", F.col("patientagegroup").cast(StringType()))
            .withColumn("patientonsetage", F.col("patientonsetage").cast(IntegerType()))
            .withColumn("patientonsetageunit", F.col("patientonsetageunit").cast(IntegerType()))
            .withColumn("patientsex", F.col("patientsex").cast(IntegerType()))
            .withColumn("patientweight", F.col("patientweight").cast(FloatType()))
            .withColumn("serious", F.col("serious").cast(IntegerType()))
            .withColumn("seriousnessdeath", F.col("seriousnessdeath").cast(IntegerType()))
            .withColumn("seriousnesshospitalization", F.col("seriousnesshospitalization").cast(IntegerType()))
            .withColumn("seriousnessdisabling", F.col("seriousnessdisabling").cast(IntegerType()))
            .withColumn("seriousnesslifethreatening", F.col("seriousnesslifethreatening").cast(IntegerType()))
            .withColumn("seriousnessother", F.col("seriousnessother").cast(IntegerType()))
            .withColumn("receivedate", F.col("receivedate").cast(StringType()))
            .withColumn("receiptdate", F.col("receiptdate").cast(StringType()))
            .withColumn("safetyreportid", F.col("safetyreportid").cast(IntegerType()))
        )   

    def transform(self):
        self.df = self.df.withColumn(
            "patientagegroup",
            (
                F
                .when(F.col("patientagegroup") == '1', "Neonate")
                .when(F.col("patientagegroup") == '2', "Infant")
                .when(F.col("patientagegroup") == '3', "Child")
                .when(F.col("patientagegroup") == '4', "Adolescent")
                .when(F.col("patientagegroup") == '5', "Adult")
                .when(F.col("patientagegroup") == '6', "Elderly")
                .otherwise(None)
             )
        )

        # Normalize patientage
        self.df = self.df.withColumn(
            "patientage(yrs)",
            (
                F
                .when(F.col("patientonsetageunit") == 800, F.col("patientonsetage") * 10)
                .when(F.col("patientonsetageunit") == 801, F.col("patientonsetage") * 1)
                .when(F.col("patientonsetageunit") == 802, F.col("patientonsetage") / 12)
                .when(F.col("patientonsetageunit") == 803, F.col("patientonsetage") / 52.143)
                .when(F.col("patientonsetageunit") == 804, F.col("patientonsetage") / 365.25)
                .when(F.col("patientonsetageunit") == 805, F.col("patientonsetage") / 8766)
                .otherwise(None)
            ).cast(FloatType())
        ).drop("patientonsetageunit", "patientonsetage")

        self.df = self.df.withColumn(
            "patientsex",
            (
                F
                .when(F.col("patientsex") == 1, "Male")
                .when(F.col("patientsex") == 2, "Female")
                .otherwise(None)
            ).cast(StringType())
        )

        self.df = self.df.withColumn(
            "patientweight",
            (
                F
                .when(
                    F.col("patientweight").rlike(r"^\d+(\.\d+)?$"),
                    F.col("patientweight").cast(FloatType()))
                .otherwise(None)
             )
        )

        self.df = self.df.withColumn(
            "serious",
            (
                F
                .when(F.col("serious") == 1, True)
                .when(F.col("serious") == 2, False)
                .otherwise(None)
             )
        )

        self.df = self.df.withColumn(
            "seriousnessdeath",
            (
                F
                .when(F.col("seriousnessdeath") == 1, True)
                .otherwise(False)
             )
        )

        self.df = self.df.withColumn(
            "seriousnesshospitalization",
            (
                F
                .when(F.col("seriousnesshospitalization") == 1, True)
                .otherwise(False)
             )
        )

        self.df = self.df.withColumn(
            "seriousnessdisabling",
            (
                F
                .when(F.col("seriousnessdisabling") == 1, True)
                .otherwise(False)
             )
        )

        self.df = self.df.withColumn(
            "seriousnesslifethreatening",
            (
                F
                .when(F.col("seriousnesslifethreatening") == 1, True)
                .otherwise(False)
             )
        )

        self.df = self.df.withColumn(
            "seriousnessother",
            (
                F
                .when(F.col("seriousnessother") == 1, True)
                .otherwise(False)
             )
        )