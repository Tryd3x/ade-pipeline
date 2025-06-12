"""Adverse Drug Event"""
import os
import uuid
import hashlib
import pandas as pd
from openfda.utilities.helper import get_module_logger

logger = get_module_logger(__name__)

class ADE:
    # Patient columns
    patient_header = [
        "patientid",
        "recordyear",
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

    # Drug columns
    drug_header = [
        "patientid",
        "recordyear",
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

    # Reaction columns
    reaction_header = [
        "patientid",
        "recordyear",
        "reactionmeddrapt",
        "reactionoutcome",
    ]

    def __init__(self, year):
        self.year = year
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
                self.year,
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
                    self.year,
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
                    self.year,
                    reaction.get("reactionmeddrapt"),
                    reaction.get("reactionoutcome"),
                ))
                
    def row_count(self,):
        df_p, df_d, df_r = self._to_dataframe()
        return df_p.shape[0], df_d.shape[0], df_r.shape[0]
                
    def _row_hash(self, df, cols_to_hash):
        df_subset = df[cols_to_hash].fillna("null").astype(str).apply(lambda col: col.str.lower())
        concatenated = df_subset.agg('|'.join, axis=1)
        df['row_hash'] = [hashlib.sha256(s.encode()).hexdigest() for s in concatenated]

        return df
    
    def _content_hash(self, df):
        row_hashes = df['row_hash'].sort_values().to_list()
        compound = "".join(row_hashes)
        
        return hashlib.sha256(compound.encode()).hexdigest()
    
    def get_hash(self):
        df_patient, df_drug, df_reaction  = self._to_dataframe(row_hash=True)
        patient_hash = self._content_hash(df_patient)
        drug_hash = self._content_hash(df_drug)
        reaction_hash = self._content_hash(df_reaction)

        return patient_hash, drug_hash, reaction_hash

    def _to_dataframe(self, row_hash = False):
        df_patient = pd.DataFrame(self.patients_list, columns=self.patient_header)
        df_drug = pd.DataFrame(self.drugs_list, columns=self.drug_header)
        df_reaction = pd.DataFrame(self.reactions_list, columns=self.reaction_header)

        if row_hash:
            df_patient = self._row_hash(df_patient, sorted(self.patient_header[1:]))
            df_drug = self._row_hash(df_drug, sorted(self.drug_header[1:]) )
            df_reaction = self._row_hash(df_reaction, sorted(self.reaction_header[1:]))
        
        return df_patient, df_drug, df_reaction

    def save_as_parquet(self, save_to, fname):
        df_patients, df_drugs, df_reactions = self._to_dataframe(row_hash=True)
        df = [df_patients, df_drugs, df_reactions]

        dirs = []

        for p in ["patient", "drug", "reaction"]:
            path = os.path.join(save_to, p, str(self.year))
            dirs.append(path)
            if not os.path.exists(path):
                logger.info(f"Directory '{path}' missing. Created '{path}'")
                os.makedirs(path, exist_ok=True)
        
        for d,p in zip(df, dirs):
            saved_path = os.path.join(p,f"{fname}.parquet")
            d.to_parquet(saved_path)
            logger.info(f"Parquet File saved to: {saved_path}")