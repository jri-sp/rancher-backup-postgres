#!/usr/bin/python
import os
import re
import sys
import time
from rancher_metadata import MetadataAPI

__author__ = 'Sebastien LANGOUREAUX'

BACKUP_DIR = '/backup/postgres'


class ServiceRun():


  def backup_duplicity_ftp(self, ftp_server, ftp_port, ftp_user, ftp_password, target_path, is_init=False):
      global BACKUP_DIR
      if ftp_server is None or ftp_server == "":
          raise KeyError("You must set the ftp server")
      if ftp_port is None:
          raise KeyError("You must set the ftp port")
      if ftp_user is None or ftp_user == "":
          raise KeyError("You must set the ftp user")
      if ftp_password is None or ftp_password == "":
          raise KeyError("You must set the ftp password")
      if target_path is None or target_path == "":
          raise KeyError("You must set the target path")

      ftp = "ftp://%s@%s:%d%s" % (ftp_user, ftp_server, ftp_port, target_path)
      cmd = "FTP_PASSWORD=%s duplicity " % (ftp_password)

      # First, we restore the last backup
      if is_init is True:
          print("Starting init the backup folder")
          os.system(cmd + '--no-encryption ' + ftp + ' ' + BACKUP_DIR + '/')


      else:
          # We backup on FTP
          print("Starting backup")
          os.system(cmd + '--no-encryption --allow-source-mismatch --full-if-older-than 7D ' + BACKUP_DIR + ' ' + ftp)

          # We clean old backup
          print("Starting cleanup")
          os.system(cmd + 'remove-all-but-n-full 3 --force --allow-source-mismatch --no-encryption ' + ftp)
          os.system(cmd + 'cleanup --force --no-encryption ' + ftp)





  def backup_postgres(self):
      global BACKUP_DIR

      # Identity database to backup
      metadata_manager = MetadataAPI()
      list_services = metadata_manager.get_service_links()
      list_postgresql = []
      for service in list_services:
          service_name = list_services[service]
          service_name_env = service_name.upper().replace('-', '_')
          database = {}
          database['host'] = service_name
          database['db'] = os.getenv(service_name_env + '_ENV_POSTGRES_DB', os.getenv(service_name_env + '_ENV_POSTGRES_USER'))
          database['user'] = os.getenv(service_name_env + '_ENV_POSTGRES_USER', 'postgres')
          database['password'] = os.getenv(service_name_env + '_ENV_POSTGRES_PASSWORD')
          database['name'] = service

          list_postgresql.append(database)
          print("Found Postgresql host to backup : " + service + " (" + service_name + ")")

      # Backup database
      for database in list_postgresql:

          cmd = 'pg_dump -h ' + database['host']

          if database['user'] is not None and database['password'] is not None:
              cmd = 'PGPASSWORD=' + database['password'] + ' ' + cmd
              cmd += ' -U ' + database['user']

          cmd += ' -d ' + database['db']
          path = BACKUP_DIR + '/' + database['name']
          os.system('mkdir -p ' + path)
          os.system('rm ' + path + '/*')
          cmd += " -f %s/postgres_%s.sql" % (path, database['db'])
          os.system(cmd)
          print("We dump " + database['db'] + " (" + database['name'] + ") in " + path)

  def set_cron(self, cron):
      if cron is None or cron == "":
          raise KeyError("You must set cron periodicity")

      with open('/etc/cron.d/backup', "w") as myFile:
          myFile.write("%s python /app/init.py \n" % (cron))



if __name__ == '__main__':
    service = ServiceRun()

    if(len(sys.argv) > 1 and sys.argv[1] == "init"):
        service.set_cron(os.getenv('CRON_SCHEDULE', '30 2 * * *'))
    else:
        service.backup_duplicity_ftp(os.getenv('FTP_SERVER'), os.getenv('FTP_PORT', 21), os.getenv('FTP_LOGIN'), os.getenv('FTP_PASSWORD'), os.getenv('FTP_TARGET_PATH', "/backup/postgres"), True)
        service.backup_postgres()
        service.backup_duplicity_ftp(os.getenv('FTP_SERVER'), os.getenv('FTP_PORT', 21), os.getenv('FTP_LOGIN'), os.getenv('FTP_PASSWORD'), os.getenv('FTP_TARGET_PATH', "/backup/postgres"))
