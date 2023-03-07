#%% Local Code Only

import socket
host_name = socket.gethostname()
if host_name == 'powerhouse':
    from dotenv import load_dotenv
    load_dotenv('./daily.env')
    # load_dotenv('./15min.env')


#%% Imports

from ddb.mysql import SQL
import os
import dlogging
from demail.gmail import SendEmail


package_name = os.getenv('package_name')
logger = dlogging.NewLogger(__file__, use_cd=True)
error_string = ''


#%% odbc connector
    
odbc = SQL()


#%% odbc proc director

director_table = os.getenv('sql_director_tbl')
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
    
    if os.getenv('email_success_send') == 'yes':
        SendEmail(to_email_addresses = os.getenv('email_success')
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