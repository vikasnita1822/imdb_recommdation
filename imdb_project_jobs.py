import kaggle
import mysql.connector 
import os
import glob
import itertools
import time
from airflow.models import Variable
rds_user = Variable.get('rds_user')
rds_password = Variable.get('rds_password')
rds_host = Variable.get('rds_host')
rds_port = Variable.get('rds_port')

def download_data():
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('whenamancodes/blood-transfusion-dataset', path='download path', unzip=True)


def local_to_rds(path,path2):
    conn = mysql.connector.connect(host=rds_host, port=rds_port,\
        user=rds_user, passwd=rds_password,allow_local_infile=True,database="database name")
    
    _path = path
    _path2 = path2
   
    # Change the directory
    os.chdir(_path)
    files = os.listdir(_path)
    tlist = []
    for i in range(0,len(files)):
        newl = files[i].split(".")
        tlist.append((newl[0] + "_" + newl[1]))

    print(tlist)

    for (fname1,tname1) in zip(glob.glob(_path2,recursive=True),tlist):
        print(fname1,"   ",tname1)

    
                        
    try:
        conn.commit()
        cur = conn.cursor()
        print('Creating table.')
        
        cur.execute("create table if not exists name_basics (nconst varchar(15), primaryName varchar(120), birthYear int, deathYear int, primaryProfession varchar(80), knownForTitles varchar(80));")
        
        cur.execute("create table if not exists title_akas (titleId varchar(15),ordering int, title varchar(900), region varchar(8), language varchar(8), types varchar(30), attributes varchar(80), isOriginalTitle tinyint);")
        
        cur.execute("create table if not exists title_basics (tconst varchar(15),titleType varchar(20), primaryTitle varchar(450),originalTitle varchar(450),isAdult int,startYear varchar(5), endYear varchar(5),runtimeMinutes int,genres varchar(40));")
        
        cur.execute("create table if not exists title_principals (tconst varchar(15),ordering int, nconst varchar(15),category varchar(30),job varchar(350),characters varchar(1400));")
        
        cur.execute("create table if not exists title_ratings (tconst varchar(15),averageRating float, numVotes int);")
        
        for (fname1,tname1) in zip(glob.glob(_path2,recursive=True),tlist):
            print(f'\n Inserting data into table {tname1}\n')
            st = time.time()
            qry1=f"LOAD DATA LOCAL INFILE '{fname1}' INTO TABLE {tname1} FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
            cur.execute(qry1)
            end=time.time()
            print("time taken:  ",str(end-st))
            conn.commit()
            print(f'Data saved successfully in {tname1}')
        

    except Exception as e:
        print(f'error | {e}')
        
    conn.close
