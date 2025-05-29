from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType
from .helper import clean_date_column

class Patient:
    columns = [
        'patientid',
        'age_group',
        'sex',
        'weight',
        'expedited_process',
        'primarysourcecountry',
        'occurcountry',
        'report_type',
        'receipt_date',
        'receive_date',
        'safetyreportid',
        'transmission_date',
        'age_years',
        'serious_type'
    ]
    
    def __init__(self, df):
        self.df = df

    def clean_date_column(self, col_name):
        
        # Fix missing days or months
        temp_df = self.df.withColumn(
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

        # Cast to datetype
        temp_df = temp_df.withColumn(col_name,(F.to_date(col_name,"yyyyMMdd")))

        return temp_df
    
    def get_df(self):
        return self.df

    def cast(self):
        self.df = (
            self.df
            .withColumn("patientid", F.col("patientid").cast(StringType()))
            .withColumn("patientagegroup", F.col("patientagegroup").cast(IntegerType()))
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
            .withColumn("safetyreportid", F.col("safetyreportid").cast(StringType()))

            # Added
            .withColumn("fulfillexpeditecriteria", F.col("fulfillexpeditecriteria").cast(IntegerType()))
            .withColumn("primarysourcecountry", F.col("primarysourcecountry").cast(StringType()))
            .withColumn("occurcountry", F.col("occurcountry").cast(StringType()))
            .withColumn("reporttype", F.col("reporttype").cast(IntegerType()))
            .withColumn("transmissiondate", F.col("transmissiondate").cast(StringType()))
            .withColumn("seriousnesscongenitalanomali", F.col("seriousnesscongenitalanomali").cast(IntegerType()))
        )   

    def transform(self):

        # Normalize patientage to years
        self.df = self.df.withColumn(
            "age_years",
            (
                F
                .when(F.col("patientonsetageunit") == 800, F.col("patientonsetage") * 10)
                .when(F.col("patientonsetageunit") == 801, F.col("patientonsetage") * 1)
                .when(F.col("patientonsetageunit") == 802, F.col("patientonsetage") / 12)
                .when(F.col("patientonsetageunit") == 803, F.col("patientonsetage") / 52.143)
                .when(F.col("patientonsetageunit") == 804, F.col("patientonsetage") / 365.25)
                .when(F.col("patientonsetageunit") == 805, F.col("patientonsetage") / (24 * 365.25))
                .otherwise(None)
            ).cast(FloatType())
        ).drop(
            "patientonsetageunit", "patientonsetage"
        )

        self.df = self.df.withColumn(
            "patientagegroup",
            (
                F
                .when((F.col("patientagegroup") == 1) | (F.col("age_years") * 365.25 < 28), F.lit("Neonate"))
                .when((F.col("patientagegroup") == 2) | ((F.col("age_years") * 365.25 >= 28) & (F.col("age_years") < 1)), F.lit("Infant"))
                .when((F.col("patientagegroup") == 3) | ((F.col("age_years") >= 1) & (F.col("age_years") <= 12)), F.lit("Child"))
                .when((F.col("patientagegroup") == 4) | ((F.col("age_years") >= 13) & (F.col("age_years") <= 17)), F.lit("Adolescent"))
                .when((F.col("patientagegroup") == 5) | ((F.col("age_years") >= 18) & (F.col("age_years") <= 64)), F.lit("Adult"))
                .when((F.col("patientagegroup") == 6) | (F.col("age_years") >= 65), F.lit("Elderly"))
                .otherwise(None)
                )
        ).withColumnRenamed("patientagegroup", "age_group")

        self.df = self.df.withColumn(
            "patientsex",
            (
                F
                .when(F.col("patientsex") == 1, F.lit("Male"))
                .when(F.col("patientsex") == 2, F.lit("Female"))
                .otherwise(None)
            ).cast(StringType())
        ).withColumnRenamed("patientsex", "sex")

        self.df = self.df.withColumn(
            "patientweight",
            (
                F
                .when(
                    F.col("patientweight").rlike(r"^\d+(\.\d+)?$"),
                    F.col("patientweight").cast(FloatType()))
                .otherwise(None)
                )
        ).withColumnRenamed("patientweight", "weight")

        self.df = self.df.withColumn(
            "serious_type",
            F.when(F.col("serious") == 1,
                F.coalesce(
                    F.when(F.col("seriousnessdeath") == 1, F.lit("Death")),
                    F.when(F.col("seriousnesscongenitalanomali") == 1, F.lit("Congenitalanomali")),
                    F.when(F.col("seriousnessdisabling") == 1, F.lit("Disabling")),
                    F.when(F.col("seriousnesshospitalization") == 1, F.lit("Hospitalization")),
                    F.when(F.col("seriousnesslifethreatening") == 1, F.lit("Lifethreatening")),
                    F.when(F.col("seriousnessother") == 1, F.lit("Other")),
                )
            ).otherwise(None)
        ).drop(
                "serious",
                "seriousnessdeath",
                "seriousnesscongenitalanomali",
                "seriousnessdisabling",
                "seriousnesshospitalization",
                "seriousnesslifethreatening",
                "seriousnessother"
            )
        
        self.df = self.clean_date_column("receivedate").withColumnRenamed("receivedate","receive_date")
        
        self.df = self.clean_date_column("receiptdate").withColumnRenamed("receiptdate","receipt_date")

        self.df = self.df.withColumn(
            "fulfillexpeditecriteria",
            (
                F
                .when(F.col('fulfillexpeditecriteria') == 1, "Yes")
                .when(F.col('fulfillexpeditecriteria') == 2, "No")
                .otherwise(None)
                )
            ).withColumnRenamed("fulfillexpeditecriteria","expedited_process")

        self.df = self.df.withColumn(
            "reporttype", 
            (
                F
                .when(F.col("reporttype") == 1, F.lit("Spontaneous"))
                .when(F.col("reporttype") == 2, F.lit("Report from study"))
                .when(F.col("reporttype") == 3, F.lit("Other"))
                .when(F.col("reporttype") == 4, F.lit("Unknown"))
                .otherwise(None)
            )
        ).withColumnRenamed("reporttype","report_type")

        self.df = self.clean_date_column("transmissiondate").withColumnRenamed("transmissiondate","transmission_date")

        # Handle null
        self.handle_null()

    def handle_null(self):
        """
        Must handle null values for all the fields not just the prone ones
        Case:
        
        - Entire field could be null
        - Entire row could be null
        - Field contains partial null
        - Row contains partial null
        """
        # TODO
        # age_group                 - Fill with 'Unspecified'
        # sex                       - Fill with 'Unknown'
        # weight                    - Fill with -1
        # expedited_process         - Fill with False
        # primarysourcecountry      - Fill with 'ZZ'
        # occurcountry              - Fill with 'ZZ'
        # report_type               - Fill with 'Unknown'
        # receipt_date              - Leave as it is
        # receive_date              - Leave as it is
        # safetyreportid            - Leave as it is
        # transmission_date         - Leave as it is
        # age_years                 - Replace NaN with -1.0
        # serious_type              - Replace with 'Not Serious'
        
        fillna_dict = {
            'age_group' : 'Unspecified',
            'sex' : 'Unknown',
            'weight' : -1,
            'expedited_process' : 'Unknown',
            'primarysourcecountry' : 'ZZ',
            'occurcountry' : 'ZZ',
            'report_type' : 'Unknown',
            'age_years' : -1.0,
            'serious_type' : 'Not Serious',
        }

        self.df = self.df.fillna(fillna_dict)

    def get_null_count(self):
        return self.df.select([
            F.sum(
                F.when(
                    F.col(c).isNull(), 1
                ).otherwise(0)
            ).alias(c)
            for c in self.df.columns
        ]).first().asDict()
    
    def get_count(self):
        return self.df.count()
