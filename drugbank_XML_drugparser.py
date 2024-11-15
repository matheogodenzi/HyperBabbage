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
        self.parsed_proteins = []
    
    def parse_drugs(self):
        for i in tqdm(range(len(self.drugs))):
            drug = self.drugs[i]
            drug_properties = self._parse_drug_properties(drug)
            self.parsed_drugs.append(drug_properties)
        return self.parse_drugs
    
    def parse_proteins(self):
        all_proteins = []
        for i in tqdm(range(len(self.drugs))):
            drug = self.drugs[i]
            proteins = self._parse_proteins(drug)
            all_proteins.extend(proteins)

        # Remove duplicates
        cleaned_proteins = []
        unique_prots = set()
        for prot in all_proteins:
            if prot['swissprot_protein_id'] not in unique_prots:
                unique_prots.add(prot['swissprot_protein_id'])
                cleaned_proteins.append(prot)

        self.parsed_proteins = cleaned_proteins
        return self.parsed_proteins
    
    def _parse_proteins(self, drug):
        proteins = []
        for i in drug:
            if 'targets' in str(i): # drug's categories
                target_features = list(i)
                for features in target_features:
                    for feature in features:
                        if 'polypeptide' in str(feature):
                            proteins_attributes = {}
                            proteins_attributes['swissprot_protein_id'] = feature.get('id')
                            for attribute in list(feature):
                                if 'name' in str(attribute):
                                    proteins_attributes['name'] = attribute.text
                                if 'general-function' in str(attribute):
                                    proteins_attributes['general-function'] = attribute.text
                                if 'specific-function' in str(attribute):
                                    proteins_attributes['specific-function'] = attribute.text
                                if 'organism' in str(attribute):
                                    proteins_attributes['organism'] = attribute.text

                            proteins.append(proteins_attributes)

        return proteins
    
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
                        drug_properties['bindingdb'] = int(ext[1].text)

            ## Data on proteins (if it is related to cancer or not ) can be added
            
        return drug_properties
        
    def save_parsed_drugs(self, output_file_drugs, output_file_proteins, return_df = False):
        parsed_drugs_df = pd.DataFrame(self.parsed_drugs)
        parsed_drugs_df.to_pickle(output_file_drugs)

        parsed_proteins_df = pd.DataFrame(self.parsed_proteins)
        parsed_proteins_df.to_pickle(output_file_proteins)

        if return_df:
            return parsed_drugs_df, parsed_proteins_df
        return None
    