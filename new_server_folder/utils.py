import os
import pymysql
import paramiko
import pandas as pd
# from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
from stat import S_IREAD
import datetime
import ipywidgets

server_db_pword = ''
server_dns = ''
server = 'new'
if server == 'old':
    server_db_pword = 'maxhom168!'
    server_dns = 'ec2-3-134-98-233.us-east-2.compute.amazonaws.com'
if server == 'new':
    server_db_pword = 'maxhom168'
    server_dns = 'ec2-3-143-213-136.us-east-2.compute.amazonaws.com'

def date_range():
    """
    Function return widgets for date range.
    """
    start_date = ipywidgets.DatePicker(
        description='Start Date',
        disabled=False,
        value=datetime.date(2020, 9, 22),
        )
    end_date = ipywidgets.DatePicker(
        description='End Date',
        disabled=False,
        value=datetime.datetime.now(),
        )
    return start_date, end_date

def db_connect(pem_file, start_date, end_date, sql_hostname='127.0.0.1',
               sql_username='root', sql_password=server_db_pword,
               sql_main_database='maxhom_qiweilian', sql_port_number=3306,
               ssh_hostname=server_dns,
               ssh_username='centos', ssh_port_number=22, sql_ip_address='1.1.1.1.1'):
    """
    Function to make connection to mysql database hosted on EC2 service on AWS.
    """

    pkeyfilepath = pem_file

    # Check if file exists
    if not os.path.exists(pkeyfilepath):
        raise FileNotFoundError("pem file ({}) missing.".format(pkeyfilepath))

    # Set given file read by the owner only.
    os.chmod(pkeyfilepath, S_IREAD)
    mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath)

    sql_host = sql_hostname
    sql_user = sql_username
    sql_passwd = sql_password
    sql_main_dbname = sql_main_database
    sql_port = sql_port_number
    ssh_host = ssh_hostname
    ssh_user = ssh_username
    ssh_port = ssh_port_number
    sql_ip = sql_ip_address

    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(sql_host, sql_port)) as tunnel:
        conn = pymysql.connect(host=sql_host, user=sql_user,
                               passwd=sql_passwd, db=sql_main_dbname,
                               port=tunnel.local_bind_port)
        query = f'''select id, status, userid, FROM_UNIXTIME(createtime) AS 'timestamp', mac, current, voltage, power, devicestatus from maxhom_power where createtime BETWEEN unix_timestamp('{start_date}') AND unix_timestamp('{end_date} 23:59:59');'''

        data = pd.read_sql_query(query, conn)
        conn.close()

        return data
