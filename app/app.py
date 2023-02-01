
# from dotenv import load_dotenv
# load_dotenv(r"I:\My Drive\MindMint\docker_run_procs\.env")


import os
import dlogging
from demail.gmail import SendEmail


package_name = os.path.basename(__file__)
logger = dlogging.NewLogger(__file__, use_cd=True)
error_string = ''


#%%

try:

    logger.info('Beginning package')


    #%% odbc connector

    from ddb.mysql import MySQL
    
    pwd_parse = os.getenv('mysql_pwd_parse')
    if pwd_parse == 'True':
        pwd_parse = True
    else:
        pwd_parse = False
    odbc = MySQL(pwd_parse=pwd_parse)
     
    
    #%% odbc env variables
    
    logger.info('Gather env dataframe')
    
    df_env = odbc.read('select * from ztlRunProcsDirector_EnvList')
    for s in range(df_env.shape[0]):
        os.environ[df_env.at[s, 'env_name']] = df_env.at[s, 'env_value']
        
    package_name = os.getenv('package_name')


    #%% odbc proc director
    
    director_table = os.getenv('sql_scrape_director_tbl')
    proc_list = odbc.read(f'SELECT proc FROM {director_table} ORDER BY sort_by;')['proc'].to_list()


    #%% run procs
    
    for proc in proc_list:
        try:
            logger.info(proc)
            odbc.run(proc)
        except Exception as e:
            e = str(e)
            error_string += e + '\n\n'


    #%% Success Email
    
    if error_string == '':
        sql = f"CALL ztpPythonLogging ('{package_name}', 1, NULL);"
        odbc.run(sql)
        
        SendEmail(os.getenv('email_success'), subject=f'{package_name} Complete', body=f'Data successfully updated for {package_name}!', user=os.getenv('yagmail_uid'), password=os.getenv('yagmail_pwd'))
    
        logger.info('Done! No problems.\n')
        
    else:
        error_string = '\n\n' + error_string
        raise Exception('Errors occured during processing!')
        
        
#%%

except Exception as e:
    e = str(e) + error_string
    logger.critical(f'{e}\n', exc_info=True)
    to_email_addresses = os.getenv('Error_ToEmail')
    body = f'Error running package {package_name}\nError message:\n{e}'
    SendEmail(to_email_addresses=to_email_addresses
                        , subject=f'Python Error - {package_name}'
                        , body=body
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('yagmail_user')
                        , password=os.getenv('yagmail_password')
                        )

    sql = f"CALL ztpPythonLogging ('{package_name}', 0, '{e}')"
    odbc.run(sql)
