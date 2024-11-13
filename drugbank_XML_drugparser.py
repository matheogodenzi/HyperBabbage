from tqdm import tqdm
import pandas as pd
from lxml import etree

class DrugParser:
    def __init__(self, xml_path):
        parser = etree.XMLParser(recover=True)
        parsed_file = etree.parse(xml_path, parser=parser)
        root = parsed_file.getroot()

        self.drugs = list(root)
        self.parsed_drugs = []
    
    def parse_drugs(self):
        for i in tqdm(range(len(self.drugs))):
            drug = self.drugs[i]
            drug_properties = self._parse_drug_properties(drug)
            self.parsed_drugs.append(drug_properties)
        return self.parse_drugs
    
    def _parse_drug_properties(self, drug):
        idDB = drug[0].text # Drug Bank ID
        drug_properties = {}
        drug_properties['id'] = idDB

        for feature in drug:
            feature_name = feature.tag

            if 'name' in feature_name: # drug name
                drug_properties['name'] = feature.text

            if 'synonyms' in feature_name: # drug's synonyms
                drug_synm = '|'.join([synm.text for synm in list(feature)])
                drug_properties['synonyms'] = drug_synm
                
            if 'toxicity' in feature_name: # drug's toxicity
                drug_properties['toxicity'] = feature.text

            if 'unii' in feature_name: # drug's UNII
                drug_properties['unii'] = feature.text

            if 'categories' in feature_name: # drug's categories
                drug_categories = '|'.join([cat[0].text for cat in list(feature)])
                drug_properties['categories'] = drug_categories

            if 'classification' in feature_name: #type of drug
                classifications = list(feature)
                drug_class_kingdom = classifications[2].text
                drug_class_superclass = classifications[3].text
                drug_properties['class_kingdom'] = drug_class_kingdom
                drug_properties['class_superclass'] = drug_class_superclass

            if 'drug-interactions' in feature_name: #interaction other drugs
                drug_interaction = '|'.join([di[0].text
                                            for di in list(feature)])
                drug_properties['interaction'] = drug_interaction
                
            if 'patents' in feature_name:
                patents_list = list(feature)
                if len(patents_list) > 0:
                    drug_patent_approved = '|'.join([cat[2].text for cat in patents_list])
                    drug_properties['patent_approved'] = drug_patent_approved

            if 'calculated-properties' in feature_name: # drug's categories
                for calc_prop in list(feature):
                    prop_name = calc_prop[0].text
                    if 'SMILES' in prop_name:
                        drug_SMILE = calc_prop[1].text
                        drug_properties['SMILES'] = drug_SMILE

                    if 'InChI' in prop_name:
                        drugInChI = calc_prop[1].text
                        drug_properties['InChI'] = drugInChI

            if 'external-identifiers' in feature_name: #other drug's IDs
                feature_list = list(feature)

                for ext in feature_list:
                    if str(ext[0].text) == 'ChEMBL':
                        drug_properties['chembl'] = ext[1].text
                    if str(ext[0].text) == 'ChEBI':
                        drug_properties['chebi'] = ext[1].text
                    if str(ext[0].text) == 'PubChem Substance':
                        drug_properties['pubchem'] = ext[1].text
                    if str(ext[0].text) == 'BindingDB':
                        drug_properties['bindingdb'] = ext[1].text
            
        return drug_properties
        
    def save_parsed_drugs(self, output_file, return_df = False):
        parsed_drugs_df = pd.DataFrame(self.parsed_drugs)
        parsed_drugs_df.to_csv(output_file, index=False, encoding='utf-8')

        if return_df:
            return parsed_drugs_df
        return None
    