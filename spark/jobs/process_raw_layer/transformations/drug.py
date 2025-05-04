from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql import functions as F

class Drug:
    drug_administration_route_map = {
        "001": "Auricular (otic)",
        "002": "Buccal",
        "003": "Cutaneous",
        "004": "Dental",
        "005": "Endocervical",
        "006": "Endosinusial",
        "007": "Endotracheal",
        "008": "Epidural",
        "009": "Extra-amniotic",
        "010": "Hemodialysis",
        "011": "Intra corpus cavernosum",
        "012": "Intra-amniotic",
        "013": "Intra-arterial",
        "014": "Intra-articular",
        "015": "Intra-uterine",
        "016": "Intracardiac",
        "017": "Intracavernous",
        "018": "Intracerebral",
        "019": "Intracervical",
        "020": "Intracisternal",
        "021": "Intracorneal",
        "022": "Intracoronary",
        "023": "Intradermal",
        "024": "Intradiscal (intraspinal)",
        "025": "Intrahepatic",
        "026": "Intralesional",
        "027": "Intralymphatic",
        "028": "Intramedullar (bone marrow)",
        "029": "Intrameningeal",
        "030": "Intramuscular",
        "031": "Intraocular",
        "032": "Intrapericardial",
        "033": "Intraperitoneal",
        "034": "Intrapleural",
        "035": "Intrasynovial",
        "036": "Intratumor",
        "037": "Intrathecal",
        "038": "Intrathoracic",
        "039": "Intratracheal",
        "040": "Intravenous bolus",
        "041": "Intravenous drip",
        "042": "Intravenous (not otherwise specified)",
        "043": "Intravesical",
        "044": "Iontophoresis",
        "045": "Nasal",
        "046": "Occlusive dressing technique",
        "047": "Ophthalmic",
        "048": "Oral",
        "049": "Oropharingeal",
        "050": "Other",
        "051": "Parenteral",
        "052": "Periarticular",
        "053": "Perineural",
        "054": "Rectal",
        "055": "Respiratory (inhalation)",
        "056": "Retrobulbar",
        "057": "Sunconjunctival",
        "058": "Subcutaneous",
        "059": "Subdermal",
        "060": "Sublingual",
        "061": "Topical",
        "062": "Transdermal",
        "063": "Transmammary",
        "064": "Transplacental",
        "065": "Unknown",
        "066": "Urethral",
        "067": "Vaginal"
    }

    def __init__(self,df):
        self.df = df

    def get_df(self):
        return self.df

    def cast(self):
        self.df = (
            self.df
            .withColumn("patientid", F.col("patientid").cast(StringType()))
            .withColumn("medicinalproduct", F.col("medicinalproduct").cast(StringType()))
            .withColumn("activesubstancename", F.col("activesubstancename").cast(StringType()))
            .withColumn("drugadministrationroute", F.col("drugadministrationroute").cast(StringType()))
            .withColumn("drugstartdate", F.col("drugstartdate").cast(StringType()))
            .withColumn("drugenddate", F.col("drugenddate").cast(StringType()))
            .withColumn("drugdosagetext", F.col("drugdosagetext").cast(StringType()))
            .withColumn("drugstructuredosagenumb", F.col("drugstructuredosagenumb").cast(FloatType()))
            .withColumn("drugstructuredosageunit", F.col("drugstructuredosageunit").cast(StringType()))
            .withColumnRenamed("drugtreatmentduration", "drugtreatmentdurationnumb")
            .withColumn("drugtreatmentdurationnumb", F.col("drugtreatmentdurationnumb").cast(IntegerType()))
            .withColumn("drugtreatmentdurationunit", F.col("drugtreatmentdurationunit").cast(StringType()))
            .withColumn("drugrecurreadministration", F.col("drugrecurreadministration").cast(IntegerType()))
            )

    def transform(self):
        # Fix missing parts of the date
        self.df = (
            self.df
            .withColumn(
                "drugstartdate",
                (
                    F
                    .when(F.length("drugstartdate") == 4, F.concat("drugstartdate",F.lit("0101")))
                    .when(F.length("drugstartdate") == 6, F.concat("drugstartdate",F.lit("01")))
                    .otherwise(F.col("drugstartdate"))
                )
            )
            .withColumn(
                "drugstartdate",
                (
                    F
                    .when(F.col("drugstartdate") < F.lit("19000101"), None)
                    .otherwise(F.col("drugstartdate"))
                )
            )
            .withColumn("drugstartdate",(F.to_date("drugstartdate","yyyyMMdd")))
        )

        self.df = (
            self.df
            .withColumn(
                "drugenddate",
                (
                    F
                    .when(F.length("drugenddate") == 4, F.concat("drugenddate",F.lit("0101")))
                    .when(F.length("drugenddate") == 6, F.concat("drugenddate",F.lit("01")))
                    .otherwise(F.col("drugenddate"))
                )
            )
                        .withColumn(
                "drugenddate",
                (
                    F
                    .when(F.col("drugenddate") < F.lit("19000101"), None)
                    .otherwise(F.col("drugenddate"))
                )
            )
            .withColumn("drugenddate",(F.to_date("drugenddate","yyyyMMdd")))
        )

        map_expr = F.create_map([F.lit(i) for i in sum(self.drug_administration_route_map.items(),())])

        self.df = (
            self.df
            .withColumn(
                "drugadministrationroute",
                map_expr[F.col("drugadministrationroute")]
            )
        )

        # Create drugstructuredosage and normalize to (mg) based on numb and unit
        self.df = (
            self.df
            .withColumn(
                "drugstructuredosage(mg)",
                (
                    F
                    .when(F.col("drugstructuredosageunit") == "001", F.col("drugstructuredosagenumb") * 1e-6)
                    .when(F.col("drugstructuredosageunit") == "002", F.col("drugstructuredosagenumb") * 1e-3)
                    .when(F.col("drugstructuredosageunit") == "003", F.col("drugstructuredosagenumb") * 1)
                    .when(F.col("drugstructuredosageunit") == "004", F.col("drugstructuredosagenumb") * 10**3)
                    .otherwise(None)
                )
            )
            .drop("drugstructuredosageunit", "drugstructuredosagenumb")
        )

        # Noramlize drugtreatmentduration to days
        self.df = (
            self.df
            .withColumn(
                "drugtreatmentduration(days)",
                (
                    F
                    .when(F.col("drugtreatmentdurationunit") == "801", F.col("drugtreatmentdurationnumb") * 365.25)
                    .when(F.col("drugtreatmentdurationunit") == "802", F.col("drugtreatmentdurationnumb") * 30.46)
                    .when(F.col("drugtreatmentdurationunit") == "803", F.col("drugtreatmentdurationnumb") * 7)
                    .when(F.col("drugtreatmentdurationunit") == "804", F.col("drugtreatmentdurationnumb") * 1)
                    .when(F.col("drugtreatmentdurationunit") == "805", F.col("drugtreatmentdurationnumb") / 24)
                    .when(F.col("drugtreatmentdurationunit") == "806", F.col("drugtreatmentdurationnumb") / 1440)
                    .otherwise(None)
                )
            )
            .drop("drugtreatmentdurationunit", "drugtreatmentdurationnumb")
        )

        self.df = (
            self.df
            .withColumn(
                "drug_reaction_after_readministration",
                (
                    F
                    .when(F.col("drugrecurreadministration") == 1, "Yes")
                    .when(F.col("drugrecurreadministration") == 2, "No")
                    .when(F.col("drugrecurreadministration") == 3, "Unknown")
                    .otherwise(None)
                )
            )
            .drop("drugrecurreadministration")
        )