#--------------------------------------------------------------------------------------------------
# Developer : Lavanya Sekar
# Date      : 08/NOV/2024
#--------------------------------------------------------------------------------------------------
import os
import sys
import pandas as pd
import re
from datetime import datetime
import cx_Oracle
import argparse
import logging
import colorlog

# Sheet and header details
sheet_name = 'Item Definition'
header_row = 6
logfile = "001_item_definition_log"

# Function to setup logging to log to a file inside the 'logs' directory with a timestamp
def setup_logging():
    log_folder = 'logs'  # Folder where the log files will be stored
    
    # Check if the 'logs' folder exists, create it if it doesn't
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    # Get current timestamp in the format DDMMYY_HHMMSS
    timestamp = datetime.now().strftime('%d%m%y_%H%M%S')
    
    # Define the log file name with the timestamp as a suffix
    log_filename = os.path.join(log_folder, f"{logfile}_{timestamp}.log")
    
    # Define log format with color placeholders for console output
    log_format = "%(log_color)s%(levelname)-8s%(reset)s [%(asctime)s] - %(message)s"
    
    # Set up a color handler for colored console output
    color_handler = colorlog.StreamHandler(sys.stdout)
    color_handler.setFormatter(colorlog.ColoredFormatter(log_format))
    
    # Create a file handler for logging to a file without color
    file_handler = logging.FileHandler(log_filename, mode='w')
    file_handler.setLevel(logging.DEBUG)  # Log everything to the file
    file_handler.setFormatter(logging.Formatter("%(levelname)-8s [%(asctime)s] - %(message)s"))

    # Set up the logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Log all levels DEBUG and above

    # Add both the color handler and file handler to the logger
    logger.addHandler(color_handler)
    logger.addHandler(file_handler) 
    
# Function to handle TABLE_NAME as command-line argument
def parse_arguments():
    parser = argparse.ArgumentParser(description="Insert data into REF table from Excel.")
    parser.add_argument('--table_name', type=str, required=True, help="Target Oracle table name.")
    parser.add_argument('--input_dir', type=str, required=True, help="Directory where the config and excel files are located")
    parser.add_argument('--config_file', type=str, required=True, help="Name of the Config file")
    parser.add_argument('--excel_file', type=str, required=True, help="Name of the Excel file")
     
     # Parse the arguments
    args = parser.parse_args()
    
    logging.info("")
    logging.info("*********************************************************************************")
    logging.info(f"BEGIN: {args.table_name} ")
    logging.info("*********************************************************************************")
    
    return args
    
# Function to get the path of the config file in the same directory as the executable
def get_config_file_path(input_dir, config_file):   
    
    # Print where it's looking for the config file
    #logging.debug(f"Current working directory: {os.getcwd()}")
    #logging.debug(f"Looking for Config file at: {os.path.join(input_dir, config_file)}")    
    
    # Check if the config file exists in the same directory as the executable
    config_file_path = os.path.join(input_dir, config_file)
    
    if os.path.exists(config_file_path):        
        return config_file_path        
    else:
        logging.error(f"Configuration file '{config_file}' not found in the directory.")
        sys.exit(1)  # Exit the script if the configuration file is not found

# Function to read configuration from the text file
def read_config(file_path):
    config = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Ignore empty lines or comments
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=')
                    config[key.strip()] = value.strip()
    except Exception as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)  # Exit the script if there's an issue with reading the config file
    return config    

# Function to get Excel file path
def get_excel_file_path(input_dir, excel_file): 
    
    # Ensure the current working directory is the directory where the executable is located
    #os.chdir(bundle_dir)  # Set the current working directory to the bundle_dir
    
    # Print where it's looking for the Excel file
    #logging.debug(f"Current working directory: {os.getcwd()}")
    #logging.debug(f"Looking for Excel file at: {os.path.join(input_dir, excel_file)}")
    
    # Check if the Excel file exists in the same directory as the .exe or .py
    excel_file_path = os.path.join(input_dir, excel_file)
    
    # If the Excel file is found, return its path
    if os.path.exists(excel_file_path):
        return excel_file_path
       
    # If Excel file is not found, raise an error or show a message
    logging.error(f"Input Excel file '{excel_file}' not found in the directory.")
    sys.exit(1)  # Exit the script with an error code (optional)

def generate_insert_statements(excel_file, sheet_name, header_row, table_name):
    logging.info("*********************************************************************************")
    logging.info(f"INPUT VALIDATION: {table_name} ")
    logging.info("*********************************************************************************")
    
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=header_row)

    # Prepare a list to hold the insert statements
    insert_statements = []
    
    # Counters for total rows and validation failures
    total_rows = len(df)
    validation_failed_count = 0   

    # List to store the row numbers of failed validations
    validation_failed_rows = []  
    sys_date = datetime.now().strftime('%d-%m-%y')

    # Loop through and create insert statements
    for index, row in df.iterrows():    
        # Perform validation for mandatory fields
        item_id = row['ITEM ID'] if pd.notna(row['ITEM ID']) else 'FAILED VALIDATION'
                
        if item_id == 'FAILED VALIDATION':            
            logging.error(f"ROW# {index + 1} : Input validation failed")
            logging.error(f"Error Message: Input ITEM ID is empty")
            validation_failed_count += 1
            validation_failed_rows.append(f"Row#{index + 1}")
            logging.info("---------------------------------------")
            continue  # Skip this row 
        
        logging.info(f"ROW# {index + 1} : Input validation successful")
        logging.info("---------------------------------------")
    
        item_id = str(row['ITEM ID']).ljust(15)
        operator_id = 114013492
        application_id = 'OL'.ljust(6)
        dl_service_code = str(0).ljust(5)
        dl_update_stamp = 0

        #008 :  : Input from excel
        item_s_desc_en = row['SHORT DESCRIPTION - 12 CHARS (ENGLISH)'] if pd.notna(row['SHORT DESCRIPTION - 12 CHARS (ENGLISH)']) else 'NULL'
        
        #009 :  : Input from excel
        item_s_desc_fr = row['SHORT DESCRIPTION - 12 CHARS (FRENCH)'] if pd.notna(row['SHORT DESCRIPTION - 12 CHARS (FRENCH)']) else 'NULL'
        
        #010 :  : Input from excel
        item_l_desc_en = row['LONG DESCRIPTION (ENGLISH)'] if pd.notna(row['LONG DESCRIPTION (ENGLISH)']) else 'NULL'
        
        #011 :  : Input from excel
        item_l_desc_fr = row['LONG DESCRIPTION (FRENCH)'] if pd.notna(row['LONG DESCRIPTION (FRENCH)']) else 'NULL'        
        
        #012 :  : Input from excel
        item_type = (
            'E' if row['ITEM TYPE'] == 'Equipment' and pd.notna(row['ITEM TYPE']) 
            else 'L' if row['ITEM TYPE'] == 'Labor' and pd.notna(row['ITEM TYPE']) 
            else 'NULL'
        )
        
        #013 :  : Input from excel
        tracking_ind = (
            'Y' if row['TRACKING INDICATION'] == 'Yes' and pd.notna(row['TRACKING INDICATION']) 
            else 'N' if row['TRACKING INDICATION'] == 'No' and pd.notna(row['TRACKING INDICATION']) 
            else 'NULL'
        )

        #014 : SERIAL_TYPE : Input from excel
        serial_type = (
            'E' if row['SERIAL TYPE'] == 'ESN (Cellular TDMA)' and pd.notna(row['SERIAL TYPE'])
            else 'G' if row['SERIAL TYPE'] == 'SIM (Cellular GSM)' and pd.notna(row['SERIAL TYPE'])
            else 'I' if row['SERIAL TYPE'] == 'IMEI (Cellular GSM)' and pd.notna(row['SERIAL TYPE'])
            else 'S' if row['SERIAL TYPE'] == 'Serialized (Messaging)' and pd.notna(row['SERIAL TYPE'])
            else 'N' if row['SERIAL TYPE'] == 'Not Serialized' and pd.notna(row['SERIAL TYPE'])
            else 'M' if row['SERIAL TYPE'] == 'IFIDO Modem' and pd.notna(row['SERIAL TYPE'])
            else 'T' if row['SERIAL TYPE'] == 'Thunder Road Device' and pd.notna(row['SERIAL TYPE'])            
            else 'P' if row['SERIAL TYPE'] == 'IP phone' and pd.notna(row['SERIAL TYPE']) 
            else 'NULL'
        )        
        
        #015 : NAM_COUNTER : Input from excel
        nam_counter = row['NAM COUNTER'] if pd.notna(row['NAM COUNTER']) else 0
        
        #016 : MANF_CD : Input from excel
        manf_cd = str(row['MANUFACTURER CODE']).ljust(10) if pd.notna(row['MANUFACTURER CODE']) else 'NULL'
        
        #017 : CORPORATE_ITEM_ID : Input from excel        
        corp_item_id = str(row['CORPORATE ITEM ID']).ljust(15) if pd.notna(row['CORPORATE ITEM ID']) else 'NULL'
        
        #018 : UNIT_TYPE : Input from excel
        unit_type  = (
            'I' if row['UNIT TYPE'] == 'Installed' and pd.notna(row['UNIT TYPE']) 
            else 'P' if row['UNIT TYPE'] == 'Portable' and pd.notna(row['UNIT TYPE'])
            else 'T' if row['UNIT TYPE'] == 'Transportable' and pd.notna(row['UNIT TYPE'])
            else 'NULL'
        )          
        
        #019 : ACCOUNTING_METHOD : Input from excel
        accounting_method  = (
            'V' if row['ACCOUNTING METHOD'] == 'Track Value' and pd.notna(row['ACCOUNTING METHOD']) 
            else 'E' if row['ACCOUNTING METHOD'] == 'Report As Expense' and pd.notna(row['ACCOUNTING METHOD']) 
            else 'N' if row['ACCOUNTING METHOD'] == 'No Accounting' and pd.notna(row['ACCOUNTING METHOD'])
            else 'NULL'
        )          
        
        #020 : UPC : Input from excel
        upc = str(row['UPC']).ljust(20) if pd.notna(row['UPC']) else 'NULL'
        
        #021 : OAA_CAPABLE : Input from excel
        oaa_capable  = (
            '1' if row['OAA CAPABLE'] == 'Capable release1' and pd.notna(row['OAA CAPABLE']) 
            else '2' if row['OAA CAPABLE'] == 'Capable release2' and pd.notna(row['OAA CAPABLE']) 
            else 'N' if row['OAA CAPABLE'] == 'Not Capable' and pd.notna(row['OAA CAPABLE']) 
            else 'NULL'
        )  
        
        #022 : OAP_CAPABLE_IND : Input from excel
        oap_capable_ind  = (
            'Y' if row['OAP CAPABLE IND'] == 'Change' and pd.notna(row['OAP CAPABLE IND']) 
            else 'N' if row['OAP CAPABLE IND'] == 'No Change' and pd.notna(row['OAP CAPABLE IND']) 
            else 'NULL'
        )  
        
        #023 : SWAP_IND : Input from excel
        swap_ind = row['SWAP IND'] if pd.notna(row['SWAP IND']) else 'NULL'
        
        #024 : UPGRADE_IND : Input from excel
        upgrade_ind = row['UPGRADE IND'] if pd.notna(row['UPGRADE IND']) else 'NULL'
        
        #025 : ALL_LOCATIONS_IND : Input from excel
        all_locations_ind  = (
            'Y' if row['ALL LOCATIONS IND'] == 'Change' and pd.notna(row['ALL LOCATIONS IND']) 
            else 'N' if row['ALL LOCATIONS IND'] == 'No Change' and pd.notna(row['ALL LOCATIONS IND']) 
            else 'NULL'
        )  
        #026 : DEF_ENCODER_TYPE : Input from excel
        def_encoder_type = str(row['DEFAULT ENCODER TYPE']) if pd.notna(row['DEFAULT ENCODER TYPE']) else 'NULL'
        
        #027 : UPGRADE_CHARGE_AMT : Input from excel
        upgrade_charge_amt = 0
        
        #028 : UPGRADE_CHARGE_AMT_1YR : Input from excel
        upgrade_charge_amt_1yr = row['UPGRADE CHARGE AMT 1YR.'] if pd.notna(row['UPGRADE CHARGE AMT 1YR.']) else 'NULL'
        
        #029 : UPGRADE_CHARGE_AMT_3YR : Input from excel
        upgrade_charge_amt_3yr = row['UPGRADE CHARGE AMT 3YR.'] if pd.notna(row['UPGRADE CHARGE AMT 3YR.']) else 'NULL'
        
        #030 : LOW_DEALER_COST : Input from excel
        low_dealer_cost = row['LOW DEALER COST'] if pd.notna(row['LOW DEALER COST']) else 'NULL'
        
        #031 : HIGH_DEALER_COST : Input from excel
        high_dealer_cost = row['HIGH DEALER COST'] if pd.notna(row['HIGH DEALER COST']) else 'NULL'
        
        #032 : DF_IND : Input from excel
        df_ind = str(row['DF IND']) if pd.notna(row['DF IND']) else 'NULL'
        
        #033 : FRANCHISE_TYPE : Input from excel
        franchise_type  = (
            'F' if row['FRANCHISE TYPE'] == 'Fido' and pd.notna(row['FRANCHISE TYPE']) 
            else 'R' if row['FRANCHISE TYPE'] == 'Rogers' and pd.notna(row['FRANCHISE TYPE']) 
            else ''
        )  
        
        #034 : FIDO_UPGR_CHG_AMT : Input from excel
        fido_upgr_chg_amt = row['FIDO UPGRADE CHARGE AMOUNT'] if pd.notna(row['FIDO UPGRADE CHARGE AMOUNT']) else 'NULL'
        
        #035 : LSB_IND : Input from excel
        lsb_ind = (
            'Y' if row['LOST STOLEN BROKEN IND'] == 'Yes' and pd.notna(row['LOST STOLEN BROKEN IND']) 
            else 'N' if row['LOST STOLEN BROKEN IND'] == 'No' and pd.notna(row['LOST STOLEN BROKEN IND']) 
            else ''
        )        
        
        #036 : RETENTION_SAVE_IND : Input from excel
        retention_save_ind = (
            'Y' if row['RETENTION SAVE IND'] == 'Yes' and pd.notna(row['RETENTION SAVE IND']) 
            else 'N' if row['RETENTION SAVE IND'] == 'No' and pd.notna(row['RETENTION SAVE IND']) 
            else ''
        )         
        
        #037 : ALS_ALLOWED : Input from excel
        als_allowed = (
            'Y' if row['ALS ALLOWED'] == 'Yes' and pd.notna(row['ALS ALLOWED']) 
            else 'N' if row['ALS ALLOWED'] == 'No' and pd.notna(row['ALS ALLOWED'])
            else 'A' if isinstance(row['ALS ALLOWED'], str) and row['ALS ALLOWED'].startswith('Additional')
            else ''
        )                
        
        #038 : SP_INSTR_REQ_IND : Input from excel
        sp_instr_req_ind = (
            'Y' if row['SP INSTR REQ IND'] == 'Yes' and pd.notna(row['SP INSTR REQ IND']) 
            else 'N' if row['SP INSTR REQ IND'] == 'No' and pd.notna(row['SP INSTR REQ IND']) 
            else ''
        )         
        
        #039 : SIM_REQ_IND : Input from excel
        sim_req_ind = (
            'Y' if row['SIM REQ IND'] == 'Yes' and pd.notna(row['SIM REQ IND']) 
            else 'N' if row['SIM REQ IND'] == 'No' and pd.notna(row['SIM REQ IND']) 
            else ''
        )         

        insert_stmt = (
            f"INSERT INTO {table_name} (ITEM_ID, SYS_CREATION_DATE, SYS_UPDATE_DATE, OPERATOR_ID, APPLICATION_ID, DL_SERVICE_CODE, DL_UPDATE_STAMP, ITEM_SDESC, ITEM_SDESC_F, ITEM_LDESC, ITEM_LDESC_F, ITEM_TYPE, TRACKING_IND, SERIAL_TYPE, NAM_COUNTER, MANF_CD, CORPORATE_ITEM_ID, UNIT_TYPE, ACCOUNTING_METHOD, UPC, OAA_CAPABLE, OAP_CAPABLE_IND, SWAP_IND, UPGRADE_IND, ALL_LOCATIONS_IND, DEF_ENCODER_TYPE, UPGRADE_CHARGE_AMT, UPGRADE_CHARGE_AMT_1YR, UPGRADE_CHARGE_AMT_3YR, LOW_DEALER_COST, HIGH_DEALER_COST, DF_IND, FRANCHISE_TYPE, FIDO_UPGR_CHG_AMT, LSB_IND, RETENTION_SAVE_IND, ALS_ALLOWED, SP_INSTR_REQ_IND, SIM_REQ_IND) "
            f"values ('{item_id}',to_date('{sys_date}','DD-MM-RR'),to_date('{sys_date}','DD-MM-RR'),{operator_id},'{application_id}','{dl_service_code}',{dl_update_stamp},'{item_s_desc_en}','{item_s_desc_fr}','{item_l_desc_en}','{item_l_desc_fr}','{item_type}','{tracking_ind}','{serial_type}',{nam_counter},'{manf_cd}','{corp_item_id}','{unit_type}','{accounting_method}','{upc}','{oaa_capable}','{oap_capable_ind}','{swap_ind}','{upgrade_ind}','{all_locations_ind}','{def_encoder_type}',{upgrade_charge_amt},{upgrade_charge_amt_1yr},{upgrade_charge_amt_3yr},{low_dealer_cost},{high_dealer_cost},'{df_ind}','{franchise_type}',{fido_upgr_chg_amt},'{lsb_ind}','{retention_save_ind}','{als_allowed}','{sp_instr_req_ind}','{sim_req_ind}')"
        )
        
        insert_statements.append(insert_stmt) 
    
    # Return all the insert statements
    return insert_statements, validation_failed_count, total_rows, validation_failed_rows

# Insert records into the database
def insert_data_to_db(insert_statements, user, password, dsn, table_name):
    insert_count = 0 
    cursor = None
    failed_count = 0
    failed_rows = []    
    connection = None
    
    try:
        # Establish the Oracle connection
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()

        # Loop through each insert statement and execute it
        for insert_stmt in insert_statements:
            try:
                cursor.execute(insert_stmt)
                insert_count += 1 
                connection.commit()
                #logging.info(f"Executed: {insert_stmt}")
            except Exception as insert_error:
                values_part = insert_stmt.split(" values ", 1)[1].strip('();')  # Split by 'values' and remove outer parentheses
                values_list = re.split(r',\s*(?![^()]*\))', values_part)  # Split values, ignoring commas inside parentheses                    
                item_id = values_list[0].strip("'")                                    
                failed_count += 1
                failed_rows.append(f"{item_id}")       
                logging.error(f"Error executing insert statement: {insert_error}")

        logging.info("*********************************************************************************")
        logging.info(f"DATABASE INSERTS: {table_name} ")
        logging.info("*********************************************************************************")
        logging.info("Insert statements executed successfully")
        logging.info(f"{insert_count} rows inserted into {table_name}")
    except cx_Oracle.DatabaseError as db_error:
        logging.error(f"Database connection failed: {db_error}")
        sys.exit(1) 
    finally:
        cursor.close()
        connection.close()
    return insert_count,failed_count,failed_rows
    
# Main execution
def main():
    
    # Setup logging first
    setup_logging()
    
    # Get command-line arguments
    args = parse_arguments()
    table_name = args.table_name
    input_dir = args.input_dir
    config_file = args.config_file
    excel_file = args.excel_file
    
    # Read configuration from the config.txt file
    config_file_path = get_config_file_path(input_dir,config_file)
    config = read_config(config_file_path)
    logging.debug(f"Config file  : {config_file_path}")

    # Retrieve the database parameters from the config file
    dsn = config.get('dsn')
    user = config.get('user')
    password = config.get('password')

    # Ensure all required parameters are provided in the config file
    if not all([dsn, user, password]):
        logging.error("Missing required configuration values.")
        sys.exit(1)    
    
    # Get Excel file path
    excel_file_path = get_excel_file_path(input_dir,excel_file)
    logging.debug(f"Input file  : {excel_file_path}")
    logging.debug(f"Input sheet : {sheet_name}")
    
    # Generate insert statements based on the Excel data
    insert_statements, validation_failed_count, total_rows, validation_failed_rows = generate_insert_statements(excel_file_path, sheet_name, header_row, table_name)
    
    # Insert the data into the database
    if insert_statements:
        inserted_row_count,failed_count,failed_rows = insert_data_to_db(insert_statements, user, password, dsn, table_name)

    # Log the summary
    logging.info("*********************************************************************************")
    logging.info(f"SUMMARY: {table_name} ")
    logging.info("*********************************************************************************")
    logging.info(f"Total rows in Excel              : {total_rows}")
    logging.info("-------------------------------------------------")
    logging.info(f"Count of successful inserts      : {inserted_row_count}")
    logging.info("-------------------------------------------------")    
    logging.info(f"Count of validation errors       : {validation_failed_count}")
    logging.info(f"Rows with validation errors      : {', '.join(validation_failed_rows)}")
    logging.info("-------------------------------------------------")
    logging.info(f"Count of failed inserts          : {failed_count}")
    logging.info(f"ITEM_ID(s) that failed to insert : {', '.join(failed_rows)}")    
    logging.info("*********************************************************************************")
    logging.info(f"END: {table_name} ")
    logging.info("*********************************************************************************")
    
if __name__ == '__main__':
    main()
