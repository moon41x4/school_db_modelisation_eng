from utils import db_modeliser

def main(data_path, script_path="script_file.sql", log_path="file.log", sec_code = {
        "DATA ENGINEERING": "ID",
        "GENE INFORMATIQUE": "GI",
        "GENE ENERGETIQUE": "GEE"
    }):
    
    mod = db_modeliser(data_path, log_path, sec_code)

    md = (mod.modelise())
    
    with open(script_path, "w") as f : 
    
        f.write(md)
    
    

if __name__ == "__main__":
    
    main('./input.csv')