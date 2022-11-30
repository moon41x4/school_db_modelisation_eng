from utils import db_modeliser
import logging

def main(data_path, script_path="script_file.sql", log_path="file.log", sec_code = {
        "DATA ENGINEERING": "ID",
        "GENIE INFORMATIQUE": "GI",
        "GENIE ENERGETIQUE": "GEE"
    }):
    
    open(log_path, "w")
    logging.basicConfig(filename=log_path,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            level=logging.INFO, 
                            datefmt='%Y-%m-%d %H:%M:%S')    
    
    logging.warning("The Progrem started")

    mod = db_modeliser(data_path, log_path, sec_code)

    try:
        md = (mod.modelise())
        with open(script_path, "w") as f : 
            f.write(md)
        logging.info("The program is working as expected")

    except:
        logging.error("An error occurred")
    
    
    

if __name__ == "__main__":
    
    
    
    main('./input.csv')