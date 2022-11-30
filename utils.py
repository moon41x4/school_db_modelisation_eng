import pandas as pd
import logging


class db_modeliser():

    def __init__(self, doc_path, log_file, sec_codes):

        self.data_path = doc_path
        self.log_file = log_file
        self.sec_codes = sec_codes
        self.start()

    def start(self):

        logging.basicConfig(filename=self.log_file,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            level=logging.INFO, 
                            datefmt='%Y-%m-%d %H:%M:%S')

        try:        
            with open(self.data_path, "r") as f:
                self.df = pd.read_csv(f)
                logging.info("The program is working as expected")
            
        except:
            logging.warning("Error Occured when trying to read the file")


    def modelise(self):
        
        '''
            this function return a script from the table with the following format
            
            SECTOR,AN,SEMESTRE,MODULE,ELEMENT,DATE_ACCREDATION
        '''

        def Inscr_Pedag_Id(id, school="ENSAH"):

                scl_id = ""
                if school == "ENSAH":
                    scl_id = "ENH"
                    
                return "INSERT INTO Inscr_Pedag_Id VALUES('{}','{}');\n".format(scl_id, id)

        def trait_notes_id(id , school_label):

                scl_tr_id = str(school_label)+"TN"

                return "INSERT INTO Trait_Notes_Id VALUES('{}','{}' ,'{}');\n".format(id, scl_tr_id, "T")
                        

        script_sql = ""
        
        for sector in self.df.SECTOR.unique():


                mod_elem_id = ""
                    
                inscr_pedag_id = ""
                
                trait_notes = ""
                
                list_element_modules = ""
                
                key_data= {}
                
                un_code = "HI"+self.sec_codes[str(sector.upper())]
                
                mod_elem_id += "--{} \n".format(sector)
                
                sec_df = self.df[self.df.SECTOR == sector ]

                for an in sec_df.AN.unique():
                    
                    keys = []

                    if an == 1:

                        mod_elem_id += "-- {}ere annee\n".format(an)
                                            
                        list_element_modules += '''           
-- modules and elements ids
insert into List_Mod_Elm_Id VALUES('{}14','O','O','VET Première Année ID','VET Première Année ID');\n'''.format(un_code)
                        
                    
                        inscr_pedag_id += '''
-- ids of inscriptions 
'''
                        trait_notes += '''
-- traitement notes for modules ids
'''
                    else:

                        id = "{}{}{}{}".format(un_code, an, 0, 4)

                        mod_elem_id += "\n\n-- {}eme annee\n".format(an)
                
                    if an == 2 :
                        list_element_modules += "insert into List_Mod_Elm_Id VALUES('{}24','O','O','VET Deuxième Année ID','VET Deuxième Année ID');\n".format(un_code)
                    
                    if an == 3 :
                        list_element_modules += "insert into List_Mod_Elm_Id VALUES('{}34','O','O','VET Troisième Année ID','VET Troisième Année ID');\n".format(un_code)
                        
                    

                    #         add key
                    id = "{}{}{}{}".format(un_code,an, 0, 4)
                    keys.append(id)

                    mod_elem_id += "insert into Mod_Elem_id values('{}', '{}', '{}', '{}', '{}');\n".format(
                        id,
                        "ENH",
                        "",
                        "VET_ELEM_NOM",
                        "VET_ELEM_NOM",
                    )
                    
                    list_element_modules += "insert into List_Mod_Elm_Id values('{}', '{}', '{}', '{}', '{}');\n".format(
                        id,
                        "O",
                        "O",
                        "VET_ELEM_NOM",
                        "VET_ELEM_NOM",
                    )
                    

                    for sem in sec_df[sec_df.AN == an].SEMESTRE.unique():

                        if pd.isna(sem):
                            continue


                        id =  "{}{}{}{}{}".format(un_code, str(sem)[-1], 0, 0, 4)

                        mod_elem_id += "-- {}\n".format(str(sem))

                        mod_elem_id += "insert into Mod_Elem_id values('{}', '{}', '{}', '{}', '{}', '{}');\n".format(
                            id,
                            "ENH",
                            "SM0" + str(sem)[-1],
                            sem,
                            "semester " + str(sem)[-1],
                            "semester " + str(sem)[-1],
                        )
                        
                        list_element_modules += "insert into  List_Mod_Elm_Id values('{}', '{}', '{}', '{}', '{}');\n".format(
                            id,
                            "O",
                            "O",
                            "semester " + str(sem)[-1],
                            "semester " + str(sem)[-1],
                        )
                        
                        #       add key
                        keys.append(id)

                        i = 0
                        for module in sec_df[sec_df.SEMESTRE == sem].MODULE:
                            j = 0

                            for elem in sec_df[sec_df.MODULE == module].ELEMENT:

                                if j == 0:

                                    id =  "{}{}{}{}{}".format(un_code, str(sem)[-1], i, j, 4)

                                    mod_elem_id += "insert into Mod_Elem_id values('{}', '{}', '{}', '{}', '{}', '{}');\n".format(
                                        id, "ENH", "MOD", sem, module, module
                                    )

                                    #        add key
                                    keys.append(id)
                                    
                                    list_element_modules += "insert into  List_Mod_Elm_Id values('{}', '{}', '{}', '{}', '{}');\n".format(
                                        id,
                                        "O",
                                        "O",
                                        module,
                                        module,
                                    )

                                if not pd.isna(elem):
                                    for sub_elem in str(elem).split("&&"):
                                        j += 1

                                        id =  "{}{}{}{}{}".format(un_code, str(sem)[-1], i, j, 4)

                                        mod_elem_id += "insert into Mod_Elem_id values('{}', '{}', '{}',  '{}', '{}', '{}');\n".format(
                                            id,
                                            "ENH",
                                            "ELM",
                                            sem,
                                            module,
                                            sub_elem,
                                        )
                                        
                                        list_element_modules += "insert into  List_Mod_Elm_Id values('{}', '{}', '{}', '{}', '{}');\n".format(
                                            id,
                                            "O",
                                            "O",
                                            module,
                                            sub_elem,
                                        )

                                        keys.append("HIID{}{}{}{}".format(str(sem)[-1], i, j, 4))

                            i += 1

                        key_data[an] = keys
                        
                        mod_elem_id += "COMMIT;\n\n"
                        
                        list_element_modules += "\n\n\n"
                        
                list_element_modules += "COMMIT;\n\n"

                mod_elem_id += "\n\n"
                for a in sec_df.AN.unique():
                    for key in key_data[a]:
                        inscr_pedag_id += Inscr_Pedag_Id(key)
                        trait_notes += trait_notes_id(key, "H")
                    inscr_pedag_id += "COMMIT; \n\n"
                    trait_notes += "COMMIT; \n\n"

                
                script_sql += mod_elem_id + inscr_pedag_id + trait_notes +list_element_modules
                
        return script_sql


class ReadFileError(Exception):
    pass


    
    