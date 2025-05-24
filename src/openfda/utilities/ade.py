"""Adverse Drug Event"""
import os
import uuid
import pandas as pd
from utilities import get_module_logger

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
        "fulfillexpeditecriteria",                           
        "primarysourcecountry",                              
        "occurcountry",                                      
        "reporttype",                                        
        "receiptdate",
        "receivedate",
        "safetyreportid",
        "transmissiondate",                                  
        "serious",
        "seriousnesscongenitalanomali",                      
        "seriousnessdeath",
        "seriousnesshospitalization",
        "seriousnessdisabling",
        "seriousnesslifethreatening",
        "seriousnessother",
    ]

    # Drug information
    drug_header = [
        "patientid",
        "actiondrug",                                     
        "drugcharacterization",                           
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
    
    def extractJSON(self, json):
        data = json.get('results')
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
                item.get("fulfillexpeditecriteria"),                            # Added
                item.get("primarysourcecountry"),                               # Added
                item.get("occurcountry"),                                       # Added
                item.get("reporttype"),                                         # Added
                item.get("receiptdate"),
                item.get("receivedate"),
                item.get("safetyreportid"),
                item.get("transmissiondate"),                                   # Added
                item.get("serious"),
                item.get("seriousnesscongenitalanomali"),                       # Added
                item.get("seriousnessdeath"),
                item.get("seriousnesshospitalization"),
                item.get("seriousnessdisabling"),
                item.get("seriousnesslifethreatening"),
                item.get("seriousnessother"),
            ))

            drugs = patient.get('drug',[])
            for drug in drugs:
                self.drugs_list.append((
                    patientid,
                    drug.get("actiondrug"),                                     # Added
                    drug.get("drugcharacterization"),                           # Added
                    

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

    def save_as_parquet(self, save_to, fname, subfolder):
        df_patients, df_drugs, df_reactions = self._to_dataframe()
        df = [df_patients, df_drugs, df_reactions]

        dirs = []

        for p in ["patient", "drug", "reaction"]:
            path = os.path.join(save_to, p, subfolder)
            dirs.append(path)
            if not os.path.exists(path):
                logger.info(f"Directory '{path}' missing. Created '{path}'")
                os.makedirs(path, exist_ok=True)
        
        for d,p in zip(df, dirs):
            saved_path = os.path.join(p,f"{fname}.parquet")
            d.to_parquet(saved_path)
            logger.info(f"Parquet File saved to: {saved_path}")