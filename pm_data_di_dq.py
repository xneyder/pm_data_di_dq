#Daniel Jaramillo August 2018

import os
import sys
import shutil
import glob
#import logging
import time
import cx_Oracle
import datetime
import subprocess
import re
from threading import Thread
from logging.handlers import TimedRotatingFileHandler
from collections import Counter
from LoggerInit import LoggerInit



class ManagedDbConnection:
	def __init__(self, DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST):
		self.DB_USER = DB_USER
		self.DB_PASSWORD = DB_PASSWORD
		self.ORACLE_SID = ORACLE_SID
		self.DB_HOST = DB_HOST

	def __enter__(self):
		app_logger=logger.get_logger('ManagedDbConnection')
		try:
			self.db = cx_Oracle.connect('{DB_USER}/{DB_PASSWORD}@{DB_HOST}/{ORACLE_SID}'.format(DB_USER=self.DB_USER,DB_PASSWORD=self.DB_PASSWORD,DB_HOST=self.DB_HOST,ORACLE_SID=self.ORACLE_SID), threaded=True)
		except cx_Oracle.DatabaseError as e:
			app_logger.error(e)
			quit()
		self.cursor = self.db.cursor()
		sqlplus_script="alter session set nls_date_format = 'DD-MON-YY HH24:MI'"
		try:
			self.cursor.execute(sqlplus_script)
		except cx_Oracle.DatabaseError as e:
			app_logger.error(e)
			app_logger.error(sqlplus_script[0:900])
			quit()
		return self.db

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.cursor:
			self.cursor.close()
		if self.db:
			self.db.close()


def check_running():
	app_logger=logger.get_logger('check_running')
        process_name=os.path.basename(sys.argv[0])
        pids=[pid for pid in os.listdir('/proc') if pid.isdigit()]
        for pid in pids:
                if int(pid) == int(os.getpid()):
                        continue
                try:
                        cmd=open(os.path.join('/proc',pid,'cmdline')).read()
                        if process_name in cmd and 'python' in cmd:
                                app_logger.error('Already running {pid} {cmd}'.format(pid=pid, cmd=cmd))
                                quit()
                except IOError:
                        continue

def run_sqlldr(ctl_file,log_file):

	"""
	Run sqlldr for the given control file.
	"""  
	p = subprocess.Popen(['sqlldr',
		'{DB_USER}/{DB_PASSWORD}@{ORACLE_SID}'.format(DB_USER=DB_USER,DB_PASSWORD=DB_PASSWORD,ORACLE_SID=ORACLE_SID),
		'control={ctl_file}'.format(ctl_file=ctl_file),
		'ERRORS=1000000',
		'log={log_file}'.format(log_file=log_file)],
	stdin=subprocess.PIPE,
	stdout=subprocess.PIPE,
	stderr=subprocess.PIPE)
	(stdout,stderr) = p.communicate()
	stdout_lines = stdout.decode('utf-8').split("\n")
	return p.returncode,stdout_lines


def load_metadata():
	app_logger=logger.get_logger('load_metadata')
	app_logger.info('Loading Metadata from {DBL_DIR}'.format(DBL_DIR=DBL_DIR))
	with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
		cursor=db.cursor()
		sqlplus_script="""
			select a.table_name,column_name,a.table_owner
			from all_ind_columns a,all_indexes b
			where a.column_position=2
			and b.uniqueness='UNIQUE'
			and a.index_name=b.index_name
		"""
		try:
			cursor.execute(sqlplus_script)
			for row in filter(None,cursor):
				keys[row[2]+'.'+row[0]]=row[1]
		except cx_Oracle.DatabaseError as e:
			app_logger.error(e)
			app_logger.error(sqlplus_script)
			quit()

		sqlplus_script="""
			select TABLE_NAME,NE_KEY_NAME,SCHEMA
			from {MT_SCHEMA}.PM_DATA_CONFIG
		""".format(MT_SCHEMA=MT_SCHEMA)
		try:
			cursor.execute(sqlplus_script)
			for row in filter(None,cursor):
				keys[row[2]+'.'+row[0]]=row[1]
		except cx_Oracle.DatabaseError as e:
			app_logger.error(e)
			app_logger.error(sqlplus_script)
			quit()
	for filename in glob.glob(DBL_DIR+'*dbl'):
		reset=True
		with open(filename,'r') as file:
			filedata=file.read().split('\n')
			for line in filedata:
				line=line.rstrip()
				if reset:
					DoneDir=None
					FailedDir=None
					ErrorDir=None
					Columns=None
					Delimiter=None
					Line_Delimiter=None
					DBProfile=None
					TargetTable=None
					reset=False
					ActiveStatus='ON'
					Date_Format="%d-%b-%y %I.%M.%S %p"
						
				if line.startswith('DBProfile'):
					DBProfile=line.split('=')[1]
				if line.startswith('DoneDir'):
					DoneDir=os.path.expandvars(line.split('=')[1].replace('$/','/'))
				if line.startswith('FailedDir'):
					FailedDir=os.path.expandvars(line.split('=')[1].replace('$/','/'))
				if line.startswith('ErrorDir'):
					ErrorDir=os.path.expandvars(line.split('=')[1].replace('$/','/'))
				if line.startswith('TargetTable'):
					TargetTable=line.split('=')[1]	
				if line.startswith('Columns'):
					Columns=line.split('=')[1].upper().split(',')
				if line.startswith('Delimiter'):
					Delimiter=line.split('=')[1]
				if line.startswith('Line_Delimiter'):
					Line_Delimiter=line.split('=')[1].replace('\\n','\n')
				if line.startswith('ActiveStatus'):
					ActiveStatus=line.split('=')[1]
				if line.startswith('Date_Format'):
					Date_Format=line.split('=')[1].replace('"','')
					if Date_Format=='AutoDateFormat':
						Date_Format="%d-%b-%y %I.%M.%S %p"
				if line.startswith('[fields]'):
					target_table=DBProfile+'.'+TargetTable
					target_table=target_table.upper()
					if ActiveStatus!='ON':
						reset=True
						app_logger.error('ActiveStatus is not ON for table {target_table} in the DBloader'.format(target_table=target_table))
						continue
					if not DoneDir and not ErrorDir and not FailedDir:
						app_logger.info('Table {target_table} DoneDir and ErrorDir and FailedDir are not defined in the DBloader'.format(target_table=target_table))
						reset=True
						continue

					if target_table not in keys:
						app_logger.info('Table {target_table} does noot have a unique key'.format(target_table=target_table))
						reset=True
						continue
			
					if DoneDir and not os.path.isdir(DoneDir):
						app_logger.error('DoneDir {DoneDir} does not exist'.format(DoneDir=DoneDir))
						reset=True
						continue

					if FailedDir and not os.path.isdir(FailedDir):
						app_logger.error('FailedDir {FailedDir} does not exist'.format(FailedDir=FailedDir))
						reset=True
						continue

					if ErrorDir and not os.path.isdir(ErrorDir):
						app_logger.error('ErrorDir {ErrorDir} does not exist'.format(ErrorDir=ErrorDir))
						reset=True
						continue

					key=keys[target_table]
					try:
						key_index=Columns.index(key)
					except ValueError:
						app_logger.error('Key {key} for table {target_table} not found in columns in the DBloader'.format(key=key,target_table=target_table))
						reset=True
						continue

					try:
						datetime_index=Columns.index('DATETIME')
					except ValueError:
						app_logger.error('DATETIME for table {target_table} not found in columns in the DBloader'.format(target_table=target_table))
						reset=True
						continue

					dbl_file=os.path.basename(filename)
						
					metadata[target_table]={'dbl_file':dbl_file,'datetime_index':datetime_index,'key_index':key_index,'key':key,'FailedDir':FailedDir,'ErrorDir':ErrorDir,'DoneDir':DoneDir,'Columns':Columns,'Delimiter':Delimiter,'Line_Delimiter':Line_Delimiter,'Date_Format':Date_Format,'dq':{}}
					donedir_list.add(DoneDir)
					errordir_list.add(ErrorDir)
					faileddir_list.add(FailedDir)
					dbl_file_list.add(dbl_file)
					schema_list.add(DBProfile)
					reset=True
	#Load DQ metadata
	with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
		cursor=db.cursor()
		sqlplus_script="""
			select SCHEMA,TABLE_NAME,NE_KEY_NAME,COUNTER,RULE
			from {MT_SCHEMA}.PM_DATA_QUALITY_CONFIG
			where ACTIVE=1
		""".format(MT_SCHEMA=MT_SCHEMA)
		try:
			cursor.execute(sqlplus_script)
			for row in filter(None,cursor):
				target_table=row[0]+'.'+row[1]
				if target_table in metadata:
					counter=row[3]
					ne_key_name=row[2]
					rule=row[4]
					try:
						key_index=metadata[target_table]['Columns'].index(ne_key_name)
						counter_index=metadata[target_table]['Columns'].index(counter)
					except ValueError:
						app_logger.error('{ne_key_name} and {counter} not found in {target_table}'.format(ne_key_name=ne_key_name,counter=counter,target_table=target_table))
						continue
					if counter in metadata[target_table]['dq']:
						metadata[target_table]['dq'][counter]['rules'].append(rule)
					else:
						metadata[target_table]['dq'][counter]={'ne_key_name':ne_key_name,'rules':[rule],'key_index':key_index,'counter_index':counter_index}
		except cx_Oracle.DatabaseError as e:
			app_logger.error(e)
			app_logger.error(sqlplus_script)
			quit()


def th_check_status(schema):
	app_logger_local=logger.get_logger('th_check_status '+schema)	
	table_list=[key.split('.')[1] for key in metadata.keys() if key.split('.')[0] == schema]
	sleep_seconds=900
	while True:
		with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
			cursor=db.cursor()
			for table in table_list:
				update_list=[]
                		app_logger_local.info('Start checking status for data in table {schema} {table}'.format(schema=schema,table=table))
				sqlplus_script="""
					WITH data_loaded AS (
					SELECT /*+ materialize index(pm_data_loaded, pm_data_loaded_idx1) */
					schema,
					table_name,
					datetime,
					ne_key_value,
					inserted_records,
					failed_records,
					substr(to_char(error_log_file),0,4000) error_log_file
					FROM
					{MT_SCHEMA}.pm_data_loaded
					WHERE
					datetime > SYSDATE - 1
					AND schema = '{schema}'
					and (table_name = '{table}' or load_type='Summary')
					)
					, lkup_b AS (
					SELECT /*+ materialize */
					schema,
					table_name,
					datetime,
					ne_key_value,
					CASE
					WHEN SUM(inserted_records) - SUM(nvl(failed_records,0) ) < 0  THEN 0
					ELSE SUM(inserted_records) - SUM(nvl(failed_records,0) )
					END
					inserted_records,
					SUM(nvl(failed_records,0) ) failed_records,
					max(error_log_file) error_log_file
					FROM
					data_loaded
					GROUP BY
					schema,
					table_name,
					datetime,
					ne_key_value
					)
					, lkup_c AS (
					SELECT /*+ materialize */
					schema,
					table_name,
					ne_key_value,
					CASE
					WHEN round(AVG(inserted_records) - AVG(nvl(failed_records,0) ) ) < 0   THEN 0
					ELSE round(AVG(inserted_records) - AVG(nvl(failed_records,0) ) )
					END
					avg_inserted_records
					FROM
					data_loaded
					GROUP BY
					schema,
					table_name,
					ne_key_value
					)
					SELECT /*+ use_hash(a) */
					nvl(b.inserted_records,0) inserted_records,
					round(nvl(c.avg_inserted_records,0) ) avg_inserted_records,
					nvl(b.failed_records,0) failed_records,
					b.error_log_file,
					CASE
					WHEN nvl(b.inserted_records,0) < c.avg_inserted_records   THEN 'Fail'
					ELSE 'Success'
					END
					status,
					a.schema,
					a.table_name,
					a.ne_key_value,
					a.datetime
					FROM
					{MT_SCHEMA}.pm_data_status a, lkup_b b, lkup_c c
					WHERE
					a.table_name = b.table_name (+)
					AND a.datetime = b.datetime (+)
					AND a.ne_key_value = b.ne_key_value (+)
					AND a.schema = '{schema}'
					AND a.table_name = c.table_name (+)
					and (a.table_name = '{table}' or load_type='Summary')
					AND a.ne_key_value = c.ne_key_value (+)
					AND a.datetime < DECODE( a.resolution, 'DY', SYSDATE - ( 1 / 1440 * 600 ), SYSDATE - ( 1 / 1440 * 300 ) )
					AND a.datetime >= SYSDATE - 1
					AND a.DATETIME_UPD_STATUS < sysdate-(1/1440*120)
					AND a.status IN ('Fail','Pending')
					ORDER BY
					a.ne_key_value,
					a.datetime
				""".format(schema=schema,table=table,MT_SCHEMA=MT_SCHEMA)
				try:
					cursor.execute(sqlplus_script)
					for row in filter(None,cursor):
						update_list.append(row)
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()

				batch_size=100000
				for start in range(0,len(update_list),batch_size):
					try:
						sqlplus_script="""
							update {MT_SCHEMA}.PM_DATA_STATUS set INSERTED_RECORDS=:1,AVG_INSERTED_RECORDS=:2,FAILED_RECORDS=:3,ERROR_LOG_FILE=:4,STATUS=:5, DATETIME_UPD_STATUS=sysdate
							WHERE SCHEMA=:6 AND TABLE_NAME=:7 AND NE_KEY_VALUE=:8 AND DATETIME=:9
						""".format(MT_SCHEMA=MT_SCHEMA)
						cursor.prepare(sqlplus_script)
						if start+batch_size > len(update_list):
							end=len(update_list)
						else:
							end=start+batch_size
						#app_logger_local.info('Executing barch {batch}'.format(batch=start))	
						cursor.executemany(None,update_list[start:end])
						db.commit()
					except cx_Oracle.DatabaseError as e:
						app_logger_local.error(e)
						app_logger_local.error(sqlplus_script)
						quit()
			
		app_logger_local.info('Sleeping {sleep_seconds} seconds'.format(sleep_seconds=sleep_seconds))
		time.sleep(sleep_seconds)

		

def th_fill_pm_status(schema):
	app_logger_local=logger.get_logger('th_fill_pm_status '+schema)	
	RESOLUTIONS={1:'5M',2:'15M',3:'HH',4:'HR',5:'DY'}
	while True:
                app_logger_local.info('Start filling PM status table for {schema}'.format(schema=schema))
		table_list=[]
		with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
			cursor=db.cursor()
			sqlplus_script="""
				SELECT SCHEMA,TABLE_NAME,RESOLUTION,NE_KEY_NAME,
				RTRIM(XMLAGG(XMLELEMENT(E,NE_KEY_VALUE,'@|@').EXTRACT('//text()') ORDER BY NE_KEY_VALUE).GetClobVal(),'@|@'), LOAD_TYPE, DBL_FILE
				FROM (SELECT DISTINCT SCHEMA, TABLE_NAME ,NE_KEY_VALUE,min(RESOLUTION) RESOLUTION,NE_KEY_NAME,LOAD_TYPE,DBL_FILE
				FROM {MT_SCHEMA}.PM_DATA_LOADED
				WHERE DATETIME > SYSDATE-1
				AND SCHEMA='{schema}'
				GROUP BY SCHEMA, TABLE_NAME ,NE_KEY_NAME, NE_KEY_VALUE,LOAD_TYPE,DBL_FILE) A
				GROUP BY SCHEMA,TABLE_NAME,RESOLUTION,NE_KEY_NAME,LOAD_TYPE,DBL_FILE
			""".format(schema=schema,MT_SCHEMA=MT_SCHEMA)
			try:
				cursor.execute(sqlplus_script)
				for row in filter(None,cursor):
					table_list.append([row[0],row[1],row[2],row[3],row[4].read().split('@|@'),row[5],row[6]])
			except cx_Oracle.DatabaseError as e:
				app_logger_local.error(e)
				app_logger_local.error(sqlplus_script)
				
		pm_status_data=[]
		tm=datetime.datetime.now()
		tm=tm.replace(microsecond=0,second=0,minute=0,hour=0)		
		for table in table_list:
			schema=table[0]
			table_name=table[1]
			resolution=table[2]
			resolution_str=RESOLUTIONS.get(table[2],'UNEFINDED')
			ne_key_name=table[3]
			instances=table[4]
			load_type=table[5]
			dbl_file=table[6]
			if resolution==1:
				#every 5 minuutes
				cycles=288
				offset=5
			elif resolution==2:
				#every 15 minuutes
				cycles=96
				offset=15
			elif resolution==3:
				#every 30 minuutes
				cycles=48
				offset=30
			elif resolution==4:
				#every 60 minuutes
				cycles=24
				offset=60
			elif resolution==5:
				#every 1440 minuutes
				cycles=1
				offset=1440
			else:
				app_logger_local.error('Resolution {resolution_str}	for table {table_name} not found'.format(resolution_str=resolution_str,table_name=table_name))
				continue
			for i in range(0,cycles):
				datetime_str=(tm+datetime.timedelta(minutes=offset*i)).strftime("%d-%b-%y %I.%M.%S %p")
				for instance in instances:
					pm_status_data.append([dbl_file,schema,table_name,datetime_str,ne_key_name,instance,'Pending',resolution_str,load_type])
		if pm_status_data:
			file_name='{TMP_DIR}/{dbl_file}-{date}.bcp'.format(dbl_file=dbl_file.replace('.dbl',''),date= datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),TMP_DIR=TMP_DIR)
			ctl_file='{TMP_DIR}/{dbl_file}-{date}.ctl'.format(dbl_file=dbl_file.replace('/','_'),date= datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),TMP_DIR=TMP_DIR)
			with open(file_name,'w') as file:
				for line in pm_status_data:
					file.write('@|@'.join(map(str,line))+'\n')
			with open(ctl_file,'w') as file:
				file.write('load data\n')
				file.write("INFILE '{file_name}'\n".format(file_name=file_name))
				file.write('INTO TABLE {MT_SCHEMA}.PM_DATA_STATUS\n'.format(MT_SCHEMA=MT_SCHEMA))
				file.write('APPEND\n')
				file.write("FIELDS TERMINATED BY '@|@'\n")
				file.write('(DBL_FILE,\n')
				file.write('SCHEMA,\n')
				file.write('TABLE_NAME,\n')
				file.write('DATETIME,\n')
				file.write('NE_KEY_NAME,\n')
				file.write('NE_KEY_VALUE,\n')
				file.write('STATUS,\n')
				file.write('RESOLUTION,\n')
				file.write('LOAD_TYPE)\n')
			app_logger_local.info("Loading {file_name}".format(file_name=file_name))
			log_file=LOG_DIR+'/'+os.path.basename(file_name.replace('.bcp','.log'))
			try:
				returncode,sqlldr_out=run_sqlldr(ctl_file, log_file)
				if returncode!=0:
					app_logger_local.error('Error loading {file_name} to table {MT_SCHEMA}.PM_DATA_STATUS'.format(file_name=file_name,MT_SCHEMA=MT_SCHEMA))
			except OSError as e:
				app_logger.error('sqlldr '+str(e))
				pass
			try:
				os.remove(log_file)
				os.remove(ctl_file)
				os.remove(file_name)
			except OSError as e:
				app_logger.error('sqlldr '+str(e))
				pass
			
		tm+=datetime.timedelta(minutes=1445)
		sleep_seconds=(tm-datetime.datetime.now()).total_seconds()
		app_logger_local.info('Sleeping {sleep_seconds} seconds'.format(sleep_seconds=sleep_seconds))
		time.sleep(sleep_seconds)

def th_process_errordir(dir):
	app_logger_local=logger.get_logger('th_process_errordir '+dir)	
	mask=dir+'/*log'
	while True:
		error_data=[]
		for logfile in glob.glob(mask):
			#Check that file is older than 1 minute
			try:
				if time.time()-os.path.getmtime(logfile) < 60:
					continue
			except OSError:
				continue
			#app_logger_local.info('Processing {logfile} file'.format(logfile=logfile))
			base_logfile=os.path.basename(logfile)
			temp=base_logfile.split('-')[1].split('.')
			schema=temp[0]
			target_table='_'.join(temp[1].split('_')[:-2])
			ftarget_table=schema+'.'+target_table
			ftarget_table=ftarget_table.upper()
			if ftarget_table not in metadata:
				app_logger_local.error('Table {ftarget_table} not found in metadata'.format(ftarget_table=ftarget_table))
				continue
			delimiter=metadata[ftarget_table]['Delimiter']
			line_delimiter=metadata[ftarget_table]['Line_Delimiter']
			datetime_index=metadata[ftarget_table]['datetime_index']
			key_index=metadata[ftarget_table]['key_index']
			dbl_file=metadata[ftarget_table]['dbl_file']
			key_name=metadata[ftarget_table]['key']
			date_format=metadata[ftarget_table]['Date_Format']
			error_messages={}
			bad_file=""
			with open(logfile,'r') as file:
				filedata=file.read()
				lines=filedata.split('\n')
				for line in lines:
					if 'Bad File' in line:
						bad_file=line.split(' ')[-1]
					elif line.startswith('ORA-'):
						error_messages[line]=1
					elif line.startswith('Record'):
						error_messages[line.split(':')[1]]=1
			if bad_file:	
				try:
					with open(bad_file,'r') as file:
						filedata=file.read()
						lines=filedata.split(line_delimiter)[:-1]
						shorted_lines=[x.split(delimiter)[datetime_index]+delimiter+x.split(delimiter)[key_index] for x in lines]
						counts=Counter(shorted_lines)
						for key,value in counts.items():
							datetime_srt=key.split(delimiter)[0]
							try:
								datetime_object = datetime.datetime.strptime(datetime_srt, date_format)	
							except ValueError:
								continue
							datetime_srt=datetime_object.strftime(date_format)
							if datetime_object.minute==5 or datetime_object.minute==10 or datetime_object.minute==20 or datetime_object.minute==25 or datetime_object.minute==35 or datetime_object.minute==40 or datetime_object.minute==50 or datetime_object.minute==55:
								resolution=1
							elif datetime_object.minute==15 or datetime_object.minute==45:
								resolution=2
							elif datetime_object.minute==30:
								resolution=3
							elif datetime_object.minute==0:
								resolution=4
							elif datetime_object.minute==0 and datetime_object.hour==0:
								resolution=5
							else:
								app_logger_local.error('Datetime {datetime_object} has invalid resolution for table {target_table} not found'.format(datetime_object=datetime_object,target_table=target_table))
								continue
							error_data.append([dbl_file,schema,target_table,datetime_srt,key_name,key.split(delimiter)[1],resolution,'\n'.join(error_messages.keys()),value])
					try:
						os.rename(logfile,logfile+'_')
					except IOError:
						pass
	
				except IOError:
					app_logger_local.error('File {bad_file} does not exist'.format(bad_file=bad_file))
					try:
						os.rename(logfile,logfile+'_')
					except IOError:
						pass

		if error_data:
			with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
	                        cursor=db.cursor()
				try:
					sqlplus_script="""
						insert into {MT_SCHEMA}.PM_DATA_LOADED (DBL_FILE,SCHEMA,TABLE_NAME,DATETIME,NE_KEY_NAME,NE_KEY_VALUE,RESOLUTION,ERROR_LOG_FILE,FAILED_RECORDS,LOAD_TYPE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,'Mediation')
					""".format(MT_SCHEMA=MT_SCHEMA)
					cursor.prepare(sqlplus_script)
					cursor.executemany(None,error_data)
					db.commit()
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()


		cycle_interval=300
		app_logger_local.info('Sleeping {cycle_interval} seconds'.format(cycle_interval=cycle_interval))
		time.sleep(cycle_interval)
		
def th_process_donedir(dir):
	app_logger_local=logger.get_logger('th_process_donedir '+dir)	
	mask=dir+'/*bcp'
	while True:
		inserted_data=[]
		fails=[]
		for bcpfile in glob.glob(mask):
			#app_logger_local.info('Processing {bcpfile} file'.format(bcpfile=bcpfile))
			base_bcpfile=os.path.basename(bcpfile)
			temp=base_bcpfile.split('-')[1].split('.')
			schema=temp[0]
			target_table=temp[1]
			ftarget_table=schema+'.'+target_table
			ftarget_table=ftarget_table.upper()
			if ftarget_table not in metadata:
				app_logger_local.error('Table {ftarget_table} not found in metadata'.format(ftarget_table=ftarget_table))
				continue
			delimiter=metadata[ftarget_table]['Delimiter']
			line_delimiter=metadata[ftarget_table]['Line_Delimiter']
			datetime_index=metadata[ftarget_table]['datetime_index']
			key_index=metadata[ftarget_table]['key_index']
			dbl_file=metadata[ftarget_table]['dbl_file']
			date_format=metadata[ftarget_table]['Date_Format']
			key_name=metadata[ftarget_table]['key']
			dq=metadata[ftarget_table]['dq']
			columns=metadata[ftarget_table]['Columns']
			try:
				with open(bcpfile,'r') as file:
					filedata=file.read()
					lines=filedata.split(line_delimiter)[:-1]
					shorted_lines=[x.split(delimiter)[datetime_index]+delimiter+x.split(delimiter)[key_index] for x in lines]
					counts=Counter(shorted_lines)
					for key,value in counts.items():
						datetime_srt=key.split(delimiter)[0]
						try:
							datetime_object = datetime.datetime.strptime(datetime_srt, date_format)			
						except (AttributeError,ValueError), e:
							app_logger_local.error('Cant parse date DATETIME: {datetime_srt} DATEFORMAT: {date_format} FILE: {bcpfile}'.format(datetime_srt=datetime_srt,date_format=date_format,bcpfile=bcpfile))
							continue
						if datetime_object.minute==5 or datetime_object.minute==10 or datetime_object.minute==20 or datetime_object.minute==25 or datetime_object.minute==35 or datetime_object.minute==40 or datetime_object.minute==50 or datetime_object.minute==55:
							resolution=1
						elif datetime_object.minute==15 or datetime_object.minute==45:
							resolution=2
						elif datetime_object.minute==30:
							resolution=3
						elif datetime_object.minute==0:
							resolution=4
						elif datetime_object.minute==0 and datetime_object.hour==0:
							resolution=5
						else:
							app_logger_local.error('Datetime {datetime_object} has invalid resolution for table {target_table} not found'.format(datetime_object=datetime_object,target_table=target_table))
							continue
						inserted_data.append([dbl_file,schema,target_table,datetime_srt,key_name,key.split(delimiter)[1],resolution,value])
					#Start checking data quality
					for counter,data in dq.items():
						dq_key_index=data['key_index']
						dq_counter_index=data['counter_index']
						for rule in data['rules']:
							test_rule=rule.replace('counter','x.split(delimiter)[dq_counter_index]')
							for x in lines:
								failed=False
								try:
									if not eval(test_rule):
										failed=True
								except ValueError:
									failed=True
								if failed:
									fails.append([schema,target_table,x.split(delimiter)[datetime_index],key_name,x.split(delimiter)[dq_key_index],counter,x.split(delimiter)[dq_counter_index],rule])
					try:
						os.remove(bcpfile)
					except OSError:
						pass
						
			except IOError:
				pass
		if inserted_data:
			with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
	                        cursor=db.cursor()
				try:
					sqlplus_script="""
						insert into {MT_SCHEMA}.PM_DATA_LOADED (DBL_FILE,SCHEMA,TABLE_NAME,DATETIME,NE_KEY_NAME,NE_KEY_VALUE,RESOLUTION,INSERTED_RECORDS,LOAD_TYPE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,'Mediation')
					""".format(MT_SCHEMA=MT_SCHEMA)
					cursor.prepare(sqlplus_script)
					cursor.executemany(None,inserted_data)
					db.commit()
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()
		if fails:
			with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
	                        cursor=db.cursor()
				try:
					sqlplus_script="""
						insert into {MT_SCHEMA}.PM_DATA_QUALITY(SCHEMA,TABLE_NAME,DATETIME,NE_KEY_NAME,NE_KEY_VALUE,COUNTER,VALUE,RULE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)
					""".format(MT_SCHEMA=MT_SCHEMA)
					cursor.prepare(sqlplus_script)
					cursor.executemany(None,fails)
					db.commit()
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					#quit()


		cycle_interval=300
		app_logger_local.info('Sleeping {cycle_interval} seconds'.format(cycle_interval=cycle_interval))
		time.sleep(cycle_interval)

def th_find_root_cause(schema):
	app_logger_local=logger.get_logger('th_find_root_cause '+schema)	
	R2L_SOURCE={
		'ALU_SAEGW':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (regexp_substr (SOURCE_FILE_NAME, '[^_]+', 1, 3), '[^.]+', 1, 1)  from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%{schema}%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ALU_CTS':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (SOURCE_FILE_NAME, '[^_]+', 1, 2) from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%{schema}%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ALU_SCG':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (SOURCE_FILE_NAME, '[^_]+', 1, 2) from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%{schema}%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ALU_IECCF':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (SOURCE_FILE_NAME, '[^_]+', 1, 2) from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%{schema}%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ALU_MRF':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (SOURCE_FILE_NAME, '[^_]+', 1, 2) from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%{schema}%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ERICSSON_EUTRAN':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (regexp_substr (SOURCE_FILE_NAME, '[^=]+', 1, 4), '[^_]+', 1, 1)  from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%ERI_EUTRAN%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'ERICSSON_MME':{'sql':"select '{table_name}'||'|'||replace(regexp_substr (SOURCE_FILE_NAME, '[^-]+', 1, 1),'A','')||'|'||regexp_substr (regexp_substr (SOURCE_FILE_NAME, '[^=]+', 1, 4), '[^,]+', 1, 1)  from audit_db.file_monitor_r2l where DESTINATION_DIRECTORY like '%ERI_MME%' and  (DESTINATION_FILE_NAME like {converted_ne_list}) and (DESTINATION_FILE_NAME like {converted_datetime_list})",'dateformat':"A%Y%m%d.%H%M"},
		'NSN_EUTRAN':{'sql':"select '{table_name}'||'|'||to_char(DATETIME,'YYYYMMDD.HH24MI')||'|'||NE_KEY_VALUE from {MT_SCHEMA}.PM_DATA_FILES where (TABLE_NAME = '{table_name}' or TABLE_NAME = 'ALL') and SCHEMA='{schema}' and (NE_KEY_VALUE in {ne_list}) and (DATETIME in ({datetime_list}))",'dateformat':"%Y%m%d.%H%M"},
		'NSN_MME':{'sql':"select '{table_name}'||'|'||to_char(DATETIME,'YYYYMMDD.HH24MI')||'|'||NE_KEY_VALUE from {MT_SCHEMA}.PM_DATA_FILES where (TABLE_NAME = '{table_name}' or TABLE_NAME = 'ALL') and SCHEMA='{schema}' and (NE_KEY_VALUE in {ne_list}) and (DATETIME in ({datetime_list}))",'dateformat':"%Y%m%d.%H%M"},
		'NSN_HSS':{'sql':"select '{table_name}'||'|'||to_char(DATETIME,'YYYYMMDD.HH24MI')||'|'||NE_KEY_VALUE from {MT_SCHEMA}.PM_DATA_FILES where (TABLE_NAME = '{table_name}' or TABLE_NAME = 'ALL') and SCHEMA='{schema}' and (NE_KEY_VALUE in {ne_list}) and (DATETIME in ({datetime_list}))",'dateformat':"%Y%m%d.%H%M"},
		'NSN_SAEGW':{'sql':"select '{table_name}'||'|'||to_char(DATETIME,'YYYYMMDD.HH24MI')||'|'||NE_KEY_VALUE from {MT_SCHEMA}.PM_DATA_FILES where (TABLE_NAME = '{table_name}' or TABLE_NAME = 'ALL') and SCHEMA='{schema}' and (NE_KEY_VALUE in {ne_list}) and (DATETIME in ({datetime_list}))",'dateformat':"%Y%m%d.%H%M"},
	}
	while True:
		holes=[]
		tables={}
		with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
			cursor=db.cursor()
			app_logger_local.info("Looking for fails to check the root cause")
			sqlplus_script="""
				select '' ROOT_CAUSE_CODE, STATUS, INSERTED_RECORDS, SCHEMA,TABLE_NAME,DATETIME,NE_KEY_VALUE, NE_KEY_NAME, AVG_INSERTED_RECORDS, ERROR_LOG_FILE
				from {MT_SCHEMA}.PM_DATA_STATUS
				where DATETIME > sysdate-1
				and status='Fail'
				and DATETIME_UPD_RC< sysdate-(1/1440*120)
				and SCHEMA='{schema}'
				and rownum<100000
				and LOAD_TYPE!='Summary'
				order by datetime desc
			""".format(schema=schema,MT_SCHEMA=MT_SCHEMA)
			try:
				cursor.execute(sqlplus_script)
				for row in filter(None,cursor):
					hole=list(row)
					table_name=hole[3]+'.'+hole[4].upper()
					if table_name not in tables:
						tables[table_name]={'datetimes':set(),'nes':set()}
					tables[table_name]['datetimes'].add(hole[5])
					tables[table_name]['nes'].add(hole[6])
					holes.append(hole)
			except cx_Oracle.DatabaseError as e:
				app_logger_local.error(e)
				app_logger_local.error(sqlplus_script)
				quit()
			data_found={}
			r2l_found={}
			for table,data in tables.items():
				ne_key_name=metadata[table]['key']
				datetime_list=','.join(["TO_DATE('"+value.strftime("%Y-%m-%d %H:%M")+"','YYYY-MM-DD HH24:MI')" for value in data['datetimes']])
				ne_list=' or {ne_key_name} in '.format(ne_key_name=ne_key_name).join(["('"+value+"')" for value in data['nes']])
				sqlplus_script="""
					select '{table}',datetime,{ne_key_name}, count(*)
					from {table}
					where datetime in ({datetime_list})
					and ({ne_key_name} in {ne_list})
					group by datetime,{ne_key_name}
				""".format(table=table,ne_key_name=ne_key_name,datetime_list=datetime_list,ne_list=ne_list)
				try:
					app_logger_local.info("Getting data for {table}".format(table=table))
					cursor.execute(sqlplus_script)
					for row in filter(None,cursor):
						record=list(row)
						table_name=record[0]
						datetime=record[1]
						ne=record[2]
						count=record[3]
						if table_name not in data_found:
							data_found[table_name]={}
						if datetime not in data_found[table_name]:
							data_found[table_name][datetime]={}
						data_found[table_name][datetime][ne]=count
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()

				if schema not in R2L_SOURCE:
					continue

				dateformat=R2L_SOURCE[schema]['dateformat']
				table_name=table.split('.')[1]
				converted_datetime_list=' or DESTINATION_FILE_NAME like '.join(["'%"+value.strftime(dateformat)+"%'" for value in data['datetimes']])
				converted_ne_list=' or DESTINATION_FILE_NAME in '.join(["'%"+value+"%'" for value in data['nes']])
				ne_list=' or NE_KEY_VALUE in '.format(ne_key_name=ne_key_name).join(["('"+value+"')" for value in data['nes']])
				sqlplus_script=R2L_SOURCE[schema]['sql'].format(schema=schema,converted_datetime_list=converted_datetime_list,table_name=table_name,datetime_list=datetime_list,ne_list=ne_list,converted_ne_list=converted_ne_list,ne_key_name=ne_key_name,MT_SCHEMA=MT_SCHEMA)
				try:
					app_logger_local.info("Getting R2L files for {table}".format(table=table))
					cursor.execute(sqlplus_script)
					for row in filter(None,cursor):
						record=list(row)
						record_key=record[0]
						if record_key not in r2l_found:
							r2l_found[record_key]=True
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()


			for idx,hole in enumerate(holes):
				schema=hole[3]
				ne_key_value=hole[6]
				datetime_object=hole[5]
				table_name=hole[4]
				ne_key_name=hole[7]
				#app_logger_local.info('Double checking if is a real hole {hole}'.format(hole=hole))
				db_records=0
				try:
					db_records=data_found[schema+'.'+table_name][datetime_object][ne_key_value]
				except KeyError:
					pass

				if db_records>hole[2]:
					holes[idx][0]=''
					holes[idx][2]=db_records
					if db_records>=hole[8]:
						holes[idx][1]='Success'	
						#app_logger_local.info('hole found in the database {hole}'.format(hole=hole))
						holes[idx]=holes[idx][:-3]
						continue

				if holes[idx][9]:
					holes[idx][0]='001'
					holes[idx]=holes[idx][:-3]
					continue

				if schema not in R2L_SOURCE:
					continue

				holes[idx]=holes[idx][:-3]
				#app_logger_local.info('Checking if raw data is available for {hole}'.format(hole=hole))
				record_key=table_name+'|'+datetime_object.strftime(dateformat)+'|'+ne_key_value
				if record_key not in r2l_found:
					holes[idx][0]='002'
				else:
					holes[idx][0]='999'

			try:
				app_logger_local.info('Updating {MT_SCHEMA}.PM_DATA_STATUS'.format(MT_SCHEMA=MT_SCHEMA))
                                sqlplus_script="""
                                        update {MT_SCHEMA}.PM_DATA_STATUS set ROOT_CAUSE_CODE=:1,STATUS=:2,INSERTED_RECORDS=:3, DATETIME_UPD_RC= sysdate  WHERE SCHEMA=:4 AND TABLE_NAME=:5 AND DATETIME=:6 AND NE_KEY_VALUE=:7
                                """.format(MT_SCHEMA=MT_SCHEMA)
                                cursor.prepare(sqlplus_script)
                                cursor.executemany(None,holes)
                                db.commit()
                        except cx_Oracle.DatabaseError as e:
                                app_logger_local.error(e)
                                app_logger_local.error(sqlplus_script)
				quit()
		if holes:
			cycle_interval=10
		else:
			cycle_interval=900

		app_logger_local.info('Sleeping {cycle_interval} seconds'.format(cycle_interval=cycle_interval))
		time.sleep(cycle_interval)

def th_find_root_cause_summary(schema):
        app_logger_local=logger.get_logger('th_find_root_cause_summary '+schema)
        while True:
                holes=[]
                tables={}
                with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
                        cursor=db.cursor()
                        app_logger_local.info("Looking for fails to check the root cause")
                        sqlplus_script="""
                                select '' ROOT_CAUSE_CODE, STATUS, INSERTED_RECORDS, SCHEMA,TABLE_NAME,DATETIME,NE_KEY_VALUE, NE_KEY_NAME, AVG_INSERTED_RECORDS, ERROR_LOG_FILE
                                from {MT_SCHEMA}.PM_DATA_STATUS
                                where DATETIME > sysdate-1
                                and status='Fail'
                                and DATETIME_UPD_RC< sysdate-(1/1440*120)
                                and SCHEMA='{schema}'
                                and rownum<100000
                                and LOAD_TYPE='Summary'
                                order by datetime desc
                        """.format(schema=schema,MT_SCHEMA=MT_SCHEMA)
                        try:
                                cursor.execute(sqlplus_script)
                                for row in filter(None,cursor):
                                        hole=list(row)
                                        table_name=hole[3]+'.'+hole[4].upper()
                                        if table_name not in tables:
                                                tables[table_name]={'datetimes':set(),'nes':set()}
                                        tables[table_name]['datetimes'].add(hole[5])
                                        tables[table_name]['nes'].add(hole[6])
                                        holes.append(hole)
                        except cx_Oracle.DatabaseError as e:
                                app_logger_local.error(e)
                                app_logger_local.error(sqlplus_script)
                                quit()
                        data_found={}
                        for table,data in tables.items():
                                datetime_list=','.join(["TO_DATE('"+value.strftime("%Y-%m-%d %H:%M")+"','YYYY-MM-DD HH24:MI')" for value in data['datetimes']])
                                sqlplus_script="""
                                        select '{table}',datetime, count(*)
                                        from {table}
                                        where datetime in ({datetime_list})
                                        group by datetime
                                """.format(table=table,datetime_list=datetime_list)
                                try:
                                        app_logger_local.info("Getting data for {table}".format(table=table))
                                        cursor.execute(sqlplus_script)
                                        for row in filter(None,cursor):
                                                record=list(row)
                                                table_name=record[0]
                                                datetime=record[1]
                                                count=record[2]
                                                if table_name not in data_found:
                                                        data_found[table_name]={}
                                                if datetime not in data_found[table_name]:
                                                        data_found[table_name][datetime]={}
                                                data_found[table_name][datetime]=count
                                except cx_Oracle.DatabaseError as e:
                                        app_logger_local.error(e)
                                        app_logger_local.error(sqlplus_script)
                                        quit()
                        for idx,hole in enumerate(holes):
                                schema=hole[3]
                                ne_key_value=hole[6]
                                datetime_object=hole[5]
                                table_name=hole[4]
                                ne_key_name=hole[7]
                                db_records=0
                                try:
                                        db_records=data_found[schema+'.'+table_name][datetime_object]
                                except KeyError:
                                        pass

                                if db_records>hole[2]:
                                        holes[idx][0]=''
                                        holes[idx][2]=db_records
                                        if db_records>=hole[8]:
                                                holes[idx][1]='Success'
                                                #app_logger_local.info('hole found in the database {hole}'.format(hole=hole))
                                                holes[idx]=holes[idx][:-3]
                                                continue
				holes[idx]=holes[idx][:-3]
                                holes[idx][0]='999'
                        try:
                                app_logger_local.info('Updating {MT_SCHEMA}.PM_DATA_STATUS'.format(MT_SCHEMA=MT_SCHEMA))
                                sqlplus_script="""
                                        update {MT_SCHEMA}.PM_DATA_STATUS set ROOT_CAUSE_CODE=:1,STATUS=:2,INSERTED_RECORDS=:3, DATETIME_UPD_RC= sysdate  WHERE SCHEMA=:4 AND TABLE_NAME=:5 AND DATETIME=:6 AND NE_KEY_VALUE=:7
                                """.format(MT_SCHEMA=MT_SCHEMA)
                                cursor.prepare(sqlplus_script)
                                cursor.executemany(None,holes)
                                db.commit()
                        except cx_Oracle.DatabaseError as e:
                                app_logger_local.error(e)
                                app_logger_local.error(sqlplus_script)
                                quit()
                if holes:
                        cycle_interval=10
                else:
                        cycle_interval=900

                app_logger_local.info('Sleeping {cycle_interval} seconds'.format(cycle_interval=cycle_interval))
                time.sleep(cycle_interval)

def fill_summary():
	app_logger_local=logger.get_logger('fill_summary')	
	while True:
		inserted_data=[]
		with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
			cursor=db.cursor()
			app_logger_local.info("Looking for summaries executed")
			sqlplus_script="""
				SELECT a.datetime run_datetime,
				b.dest_tablename,
				SUBSTR(a.message, INSTR(a.message, 'Inserted') + 9, INSTR(a.message, 'rows') - INSTR(a.message, 'Inserted') - 9) rows_processed,
				SUBSTR(a.message, INSTR(a.message, 'from') + 5, INSTR(a.message, ' to ',INSTR(a.message, 'from')) - INSTR(a.message, 'from') - 5) datetime,
				message
				FROM logs.summary_log a, aircom.summary_reports b
				WHERE a.datetime >= (select last_handled_datestamp from {MT_SCHEMA}.pm_data_summary_lh)
				AND a.msg_number in (109)
				AND a.prid = b.prid
				AND a.message not like '%No new managed elements%'
				and SUBSTR(a.message, INSTR(a.message, 'Inserted') + 9, INSTR(a.message, 'rows') - INSTR(a.message, 'Inserted') - 9) > 0
				order by a.datetime 
			""".format(MT_SCHEMA=MT_SCHEMA)
			try:
				date_format='%d/%m/%Y %H:%M:%S'
				cursor.execute(sqlplus_script)
				for row in filter(None,cursor):
					row=list(row)
					last_handled_datestamp=row[0]
					schema=row[1].split('.')[0]
					target_table=row[1].split('.')[1]
					records=row[2]
					datetime_srt=row[3]
					message=row[4]
					try:
						datetime_object = datetime.datetime.strptime(datetime_srt, date_format)
					except (AttributeError,ValueError), e:
						app_logger_local.error('Cant parse date DATETIME: {datetime_srt} DATEFORMAT: {date_format}'.format(datetime_srt=datetime_srt,date_format=date_format))	
						continue

					if datetime_object.minute==5 or datetime_object.minute==10 or datetime_object.minute==20 or datetime_object.minute==25 or datetime_object.minute==35 or datetime_object.minute==40 or datetime_object.minute==50 or datetime_object.minute==55:
						resolution=1
					elif datetime_object.minute==15 or datetime_object.minute==45:
						resolution=2
					elif datetime_object.minute==30:
						resolution=3
					elif datetime_object.minute==0:
						resolution=4
					elif datetime_object.minute==0 and datetime_object.hour==0:
						resolution=5
					else:
						app_logger_local.error('Datetime {datetime_object} has invalid resolution for table {target_table} not found'.format(datetime_object=datetime_object,target_table=target_table))
						continue
					inserted_data.append(['NA',schema,target_table,datetime_object,'ALL','ALL',resolution,records,message])
			except cx_Oracle.DatabaseError as e:
				app_logger_local.error(e)
				app_logger_local.error(sqlplus_script)
				quit()
			data_found={}

		if inserted_data:
			with ManagedDbConnection(DB_USER,DB_PASSWORD,ORACLE_SID,DB_HOST) as db:
	                        cursor=db.cursor()
				try:
					sqlplus_script="""
						insert into {MT_SCHEMA}.PM_DATA_LOADED (DBL_FILE,SCHEMA,TABLE_NAME,DATETIME,NE_KEY_NAME,NE_KEY_VALUE,RESOLUTION,INSERTED_RECORDS,ERROR_LOG_FILE,LOAD_TYPE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,'Summary')
					""".format( MT_SCHEMA=MT_SCHEMA)
					cursor.prepare(sqlplus_script)
					cursor.executemany(None,inserted_data)
					db.commit()
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()

				sqlplus_script="""
					update {MT_SCHEMA}.pm_data_summary_lh set last_handled_datestamp=TO_DATE('{last_handled_datestamp}','YYYY-MM-DD HH24:MI:SS')
				""".format(last_handled_datestamp=last_handled_datestamp.strftime("%Y-%m-%d %H:%M:%S"),
					MT_SCHEMA=MT_SCHEMA)
				try:
					cursor.execute(sqlplus_script)
					db.commit()
				except cx_Oracle.DatabaseError as e:
					app_logger_local.error(e)
					app_logger_local.error(sqlplus_script)
					quit()
		cycle_interval=300
		app_logger_local.info('Sleeping {cycle_interval} seconds'.format(cycle_interval=cycle_interval))
		time.sleep(cycle_interval)

		
def main():
        app_logger=logger.get_logger('Main')
	check_running()
	load_metadata()
	workers=[]

	for donedir in filter(None,donedir_list):
		app_logger.info('Start monitoring {donedir} for bcp files'.format(donedir=donedir))
		worker = Thread(target=th_process_donedir, args=(donedir,))
		worker.setDaemon(True)
		workers.append({'function':th_process_donedir,'params':donedir,'object':worker})
		worker.start()
	for faileddir in filter(None,faileddir_list):
		app_logger.info('Start monitoring {faileddir} for bcp files'.format(faileddir=faileddir))
		worker = Thread(target=th_process_donedir, args=(faileddir,))
		worker.setDaemon(True)
		workers.append({'function':th_process_donedir,'params':faileddir,'object':worker})
		worker.start()
	for errordir in filter(None,errordir_list):
		app_logger.info('Start monitoring {errordir} for log files'.format(errordir=errordir))
		worker = Thread(target=th_process_errordir, args=(errordir,))
		worker.setDaemon(True)
		workers.append({'function':th_process_errordir,'params':errordir,'object':worker})
		worker.start()
	for schema in filter(None,schema_list):
		worker = Thread(target=th_fill_pm_status, args=(schema,))
		worker.setDaemon(True)
		workers.append({'function':th_fill_pm_status,'params':schema,'object':worker})
		worker.start()
	#schema_list=['ERICSSON_EUTRAN']
	for schema in filter(None,schema_list):
		worker = Thread(target=th_check_status, args=(schema,))
		worker.setDaemon(True)
		workers.append({'function':th_check_status,'params':schema,'object':worker})
		worker.start()
	for schema in filter(None,schema_list):
		worker = Thread(target=th_find_root_cause, args=(schema,))
		worker.setDaemon(True)
		workers.append({'function':th_find_root_cause,'params':schema,'object':worker})
		worker.start()
	for schema in filter(None,schema_list):
		worker = Thread(target=th_find_root_cause_summary, args=(schema,))
		worker.setDaemon(True)
		workers.append({'function':th_find_root_cause_summary,'params':schema,'object':worker})
		worker.start()

	worker = Thread(target=fill_summary, args=())
	worker.setDaemon(True)
	worker.start()
	workers.append({'function':fill_summary,'params':'','object':worker})


	#sleep forever
	while True:
		time.sleep(90000)

if __name__ == "__main__":
	TMP_DIR=os.environ['TMP_DIR']+'/pm_data_di_dq/'
	if not os.path.exists(TMP_DIR):
		os.makedirs(TMP_DIR)
        LOG_DIR=os.environ['LOG_DIR']+'/pm_data_di_dq/'
	if not os.path.exists(LOG_DIR):
		os.makedirs(LOG_DIR)
        LOG_FILE=LOG_DIR+'/pm_data_di_dq.log'
	logger=LoggerInit(LOG_FILE,10)
	DB_USER=os.environ['DB_USER']
	DB_PASSWORD=os.environ['DB_PASSWORD']
	ORACLE_SID=os.environ['ORACLE_SID']
	DB_HOST=os.environ['DB_HOST']
	DBL_DIR=os.environ['DVX2_IMP_DIR']+'/config/Dbl/'
	MT_SCHEMA=os.environ['SCHEMA']
	dbl_file_list=set()
	metadata={}
	donedir_list=set()
	faileddir_list=set()
	errordir_list=set()
	schema_list=set()
	keys={}
        main()

