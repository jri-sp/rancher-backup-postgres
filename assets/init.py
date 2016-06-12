#!/usr/bin/python
import os
import re
import sys
import time
from datetime import datetime
from rancher_metadata import MetadataAPI

__author__ = 'Sebastien LANGOUREAUX'

BACKUP_DIR = '/backup/postgres'


class ServiceRun():

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
          now = datetime.now()
          cmd += " -f %s/postgres_%s_%d-%d-%d_%d-%d-%d.sql" % (path, database['db'], now.year, now.month, now.day, now.hour, now.minute, now.second)
          os.system(cmd)
          print("We dump " + database['db'] + " (" + database['name'] + ") in " + path)

if __name__ == '__main__':
    # Start


    service = ServiceRun()
    service.backup_postgres()
