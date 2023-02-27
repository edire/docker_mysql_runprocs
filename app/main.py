#%% Local Code Only

import socket
host_name = socket.gethostname()
if host_name == 'powerhouse':
    from dotenv import load_dotenv
    load_dotenv('./.env')
    load_dotenv('../.env')


#%% Imports

from ddb.mysql import SQL
import os
import dlogging
from demail.gmail import SendEmail


package_name = os.path.basename(__file__)
logger = dlogging.NewLogger(__file__, use_cd=True)
error_string = ''

#%% odbc connector
    
odbc = SQL()
    

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
        logger.warning(e)


#%% Success Email

if error_string == '':
    sql = f"CALL ztpPythonLogging ('{package_name}', 1, NULL);"
    odbc.run(sql)
    
    SendEmail(to_email_addresses = os.getenv('email_send')
              , subject=f'{package_name} Complete'
              , body=f'Data successfully updated for {package_name}!'
              , user=os.getenv('email_uid')
              , password=os.getenv('email_pwd')
              )
    
else:
    e = error_string.replace("'", "")
    sql = f"CALL ztpPythonLogging ('{package_name}', 0, '{e}')"
    odbc.run(sql)

    raise Exception('Errors occured during processing!\n\n' + error_string)