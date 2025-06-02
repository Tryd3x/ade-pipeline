from pyspark.sql.types import StringType, FloatType, IntegerType, LongType
from pyspark.sql import functions as F
from .helper import clean_date_column

class Drug:
    columns = [
        'patientid',
        'recordyear',
        'actiondrug',
        'drugcharacterization',
        'medicinalproduct',
        'activesubstancename',
        'drug_indication',
        'administration_route',
        'drug_start_date',
        'drug_end_date',
        'drugdosagetext',
        'dosage_mg',
        'treatment_duration_days',
        'drug_reaction_after_readministration'
    ]

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
            .withColumn("recordyear", F.col("recordyear").cast(StringType()))
            .withColumn("medicinalproduct", F.col("medicinalproduct").cast(StringType()))
            .withColumn("activesubstancename", F.col("activesubstancename").cast(StringType()))
            .withColumn("drugindication", F.col("drugindication").cast(StringType()))
            .withColumn("drugadministrationroute", F.col("drugadministrationroute").cast(StringType()))
            .withColumn("drugstartdate", F.col("drugstartdate").cast(StringType()))
            .withColumn("drugenddate", F.col("drugenddate").cast(StringType()))
            .withColumn("drugdosagetext", F.col("drugdosagetext").cast(StringType()))
            .withColumn("drugstructuredosagenumb", F.col("drugstructuredosagenumb").cast(FloatType()))
            .withColumn("drugstructuredosageunit", F.col("drugstructuredosageunit").cast(StringType()))
            .withColumn("drugtreatmentduration", F.col("drugtreatmentduration").cast(IntegerType()))
            .withColumn("drugtreatmentdurationunit", F.col("drugtreatmentdurationunit").cast(StringType()))
            .withColumn("drugrecurreadministration", F.col("drugrecurreadministration").cast(IntegerType()))
            .withColumn("actiondrug", F.col("actiondrug").cast(IntegerType()))
            .withColumn("drugcharacterization", F.col("drugcharacterization").cast(IntegerType()))
            )

    def transform(self):
        # Fix date
        self.df = clean_date_column(self.df, "drugstartdate").withColumnRenamed("drugstartdate", "drug_start_date")

        self.df = clean_date_column(self.df, "drugenddate").withColumnRenamed("drugenddate", "drug_end_date")

        map_expr = F.create_map([F.lit(i) for i in sum(self.drug_administration_route_map.items(),())])

        self.df = self.df.withColumn("drugadministrationroute", map_expr[F.col("drugadministrationroute")]).withColumnRenamed("drugadministrationroute", "administration_route")

        # Find and replace strings containing the word "Unknown"
        self.df = self.df.withColumn(
            "drugindication",
            F.when(
                F.col('drugindication').rlike("(?i)Unknown"),
                F.lit("Unknown")
            ).otherwise(F.col('drugindication'))
        ).withColumn(
            "drugindication",
            F.regexp_replace("drugindication",r"\^", "'")
        ).withColumnRenamed("drugindication","drug_indication")
        
        # Normalize dosage to mg
        self.df = self.df.withColumn(
            "dosage_mg",
            (
                F
                .when(F.col("drugstructuredosageunit") == "001", F.col("drugstructuredosagenumb") * 1e6)
                .when(F.col("drugstructuredosageunit") == "002", F.col("drugstructuredosagenumb") * 1e3)
                .when(F.col("drugstructuredosageunit") == "003", F.col("drugstructuredosagenumb") * 1)
                .when(F.col("drugstructuredosageunit") == "004", F.col("drugstructuredosagenumb") * 1e-3)
                .otherwise(None)
            )
        ).drop("drugstructuredosageunit", "drugstructuredosagenumb")

        # Noramlized to days
        self.df = self.df.withColumn(
            "treatment_duration_days",
            (
                F
                .when(F.col("drugtreatmentdurationunit") == "801", F.col("drugtreatmentduration") * 365.25)
                .when(F.col("drugtreatmentdurationunit") == "802", F.col("drugtreatmentduration") * 30.46)
                .when(F.col("drugtreatmentdurationunit") == "803", F.col("drugtreatmentduration") * 7)
                .when(F.col("drugtreatmentdurationunit") == "804", F.col("drugtreatmentduration") * 1)
                .when(F.col("drugtreatmentdurationunit") == "805", F.col("drugtreatmentduration") / 24)
                .when(F.col("drugtreatmentdurationunit") == "806", F.col("drugtreatmentduration") / 1440)
                .otherwise(None)
            )
        ).drop("drugtreatmentdurationunit", "drugtreatmentduration")
        

        self.df = self.df.withColumn(
            "drug_reaction_after_readministration",
            (
                F
                .when(F.col("drugrecurreadministration") == 1, F.lit("Yes"))
                .when(F.col("drugrecurreadministration") == 2, F.lit("No"))
                .when(F.col("drugrecurreadministration") == 3, F.lit("Unknown"))
                .otherwise(None)
            )
        ).drop("drugrecurreadministration")
        

        self.df = self.df.withColumn(
            "actiondrug",
            (
                F
                .when(F.col("actiondrug") == 1, F.lit("Drug withdrawn"))
                .when(F.col("actiondrug") == 2, F.lit("Dose reduced"))
                .when(F.col("actiondrug") == 3, F.lit("Dose increased"))
                .when(F.col("actiondrug") == 4, F.lit("Dose not changed"))
                .when(F.col("actiondrug") == 5, F.lit("Unknown"))
                .when(F.col("actiondrug") == 6, F.lit("Not applicable"))
                .otherwise(None)
            )
        )

        self.df = self.df.withColumn(
            "drugcharacterization",
            (
                F
                .when(F.col("drugcharacterization") == 1, F.lit("Suspect"))
                .when(F.col("drugcharacterization") == 2, F.lit("Concomitant"))
                .when(F.col("drugcharacterization") == 3, F.lit("Interacting"))
                .otherwise(None)
            )
        )

        # Handle null
        self.handle_null()

    def handle_null(self):
        fillna_dict = { 
            'actiondrug' : 'Unknown',
            'drugcharacterization': 'Unknown',
            'medicinalproduct': 'Unknown',
            'activesubstancename': 'Unknown',
            'drug_indication': 'Unknown',
            'administration_route': 'Unknown',
            # 'drug_start_date': '',
            # 'drug_end_date': '',
            'drugdosagetext': 'Not Specified',
            'dosage_mg': -1.0,
            'treatment_duration_days': -1,
            'drug_reaction_after_readministration': 'Unknown',

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