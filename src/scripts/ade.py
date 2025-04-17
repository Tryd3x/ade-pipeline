"""Adverse Drug Event"""
import os
import uuid
import pandas as pd
from utilities.logger_config import get_module_logger

logger = get_module_logger(__name__)

class ADE:
    # Patient information
    patient_header = [
        "patientid",
        "patientagegroup",
        "patientonsetage",
        "patientonsetageunit",
        "patientsex",
        "patientweight",
        "serious",
        "seriousnessdeath",
        "seriousnesshospitalization",
        "seriousnessdisabling",
        "seriousnesslifethreatening",
        "seriousnessother",
        "receivedate",
        "receiptdate",
        "safetyreportid"
    ]

    # Drug information
    drug_header = [
        "patientid",
        "medicinalproduct",
        "activesubstancename",
        "drugindication",
        "drugadministrationroute",
        "drugstartdate",
        "drugenddate",
        "drugdosagetext",
        "drugstructuredosagenumb",
        "drugstructuredosageunit",
        "drugtreatmentduration",
        "drugtreatmentdurationunit",
        "drugrecurreadministration",
    ]

    # Reaction information
    reaction_header = [
        "patientid",
        "reactionmeddrapt",
        "reactionoutcome",
    ]

    def __init__(self):
        self.patients_list = []
        self.drugs_list = []
        self.reactions_list = []
    
    def extractJSON(self, data):
        for item in data:
            patientid = str(uuid.uuid4())
            patient = item.get("patient",{})

            self.patients_list.append((
                patientid,
                patient.get("patientagegroup"),
                patient.get("patientonsetage"),
                patient.get("patientonsetageunit"),
                patient.get("patientsex"),
                patient.get("patientweight"),
                patient.get("serious"),
                patient.get("seriousnessdeath"),
                patient.get("seriousnesshospitalization"),
                patient.get("seriousnessdisabling"),
                patient.get("seriousnesslifethreatening"),
                patient.get("seriousnessother"),
                patient.get("receivedate"),
                patient.get("receiptdate"),
                patient.get("safetyreportid"),
            ))

            drugs = patient.get('drug',[])
            for drug in drugs:
                self.drugs_list.append((
                    patientid,
                    drug.get("medicinalproduct"),
                    drug.get("activesubstance",{}).get("activesubstancename"),
                    drug.get("drugindication"),    
                    drug.get("drugadministrationroute"),    
                    drug.get("drugstartdate"),
                    drug.get("drugenddate"),
                    drug.get("drugdosagetext"),
                    drug.get("drugstructuredosagenumb"),
                    drug.get("drugstructuredosageunit"),
                    drug.get("drugtreatmentduration"),
                    drug.get("drugtreatmentdurationunit"),
                    drug.get("drugrecurreadministration"),
                ))

            reactions = patient.get("reaction",[])
            for reaction in reactions:
                self.reactions_list.append((
                    patientid,
                    reaction.get("reactionmeddrapt"),
                    reaction.get("reactionoutcome"),
                ))

    def _to_dataframe(self):
        df_patients = pd.DataFrame(self.patients_list, columns=self.patient_header)
        df_drugs = pd.DataFrame(self.drugs_list, columns=self.drug_header)
        df_reactions = pd.DataFrame(self.reactions_list, columns=self.reaction_header)

        return df_patients, df_drugs, df_reactions

    def save_as_parquet(self, fname, dir):
        df_patients, df_drugs, df_reactions = self._to_dataframe()
        df = [df_patients, df_drugs, df_reactions]

        dirs = []

        for p in ["patient", "drug", "reaction"]:
            path = os.path.join("pq", p, dir)
            dirs.append(path)
            os.makedirs(path, exist_ok=True)
        
        for d,p in zip(df, dirs):
            saved_path = os.path.join(p,f"{fname}.parquet")
            d.to_parquet(saved_path)
            logger.info(f"Parquet File saved to: {saved_path}")