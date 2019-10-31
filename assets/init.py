#!/usr/bin/python
import os
import re
import sys
import time
from rancher_metadata import MetadataAPI

__author__ = 'Sebastien LANGOUREAUX'

BACKUP_DIR = '/backup/postgres'


class ServiceRun():

  def get_docker_secret(self, var, default=None):
      secretfile_var = var + '_FILE'
      val = default
      if os.getenv(var) is not None and os.getenv(var) != "":
          val = os.getenv(var)
      elif os.getenv(secretfile_var) is not None and os.getenv(secretfile_var) != "":
          with open(os.getenv(secretfile_var), 'r') as secret_file:
              val = secret_file.read()
      return val

  def backup_duplicity_ftp(self, backend, target_path, full_backup_frequency, nb_full_backup_keep, nb_increment_backup_chain_keep, volume_size, is_init=False):
      global BACKUP_DIR
      if backend is None or backend == "":
          raise KeyError("You must set the target backend")
      if target_path is None or target_path == "":
          raise KeyError("You must set the target path")
      if full_backup_frequency is None or full_backup_frequency == "":
          raise KeyError("You must set the full backup frequency")
      if nb_full_backup_keep is None or nb_full_backup_keep == "":
          raise KeyError("You must set how many full backup you should to keep")
      if nb_increment_backup_chain_keep is None or nb_increment_backup_chain_keep == "":
          raise KeyError("You must set how many incremental chain with full backup you should to keep")
      if volume_size is None or volume_size == "":
          raise KeyError("You must set the volume size")

      backend = "%s%s" % (backend, target_path)
      cmd = "duplicity"

      # First, we restore the last backup
      if is_init is True:
          print("Starting init the backup folder")
          os.system("%s --no-encryption %s %s" % (cmd, backend, BACKUP_DIR))


      else:
          # We backup on FTP
          print("Starting backup")
          os.system("%s --volsize %s --no-encryption --allow-source-mismatch --full-if-older-than %s %s %s" % (cmd, volume_size, full_backup_frequency, BACKUP_DIR, backend))

          # We clean old backup
          print("Starting cleanup")
          os.system("%s remove-all-but-n-full %s --force --allow-source-mismatch --no-encryption %s" % (cmd, nb_full_backup_keep, backend))
          os.system("%s remove-all-inc-of-but-n-full %s --force --allow-source-mismatch --no-encryption %s" % (cmd, nb_increment_backup_chain_keep, backend))
          os.system("%s cleanup --force --no-encryption %s" % (cmd, backend))


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
          database['db'] = get_docker_secret(service_name_env + '_ENV_POSTGRES_DB', get_docker_secret(service_name_env + '_ENV_POSTGRES_USER'))
          database['user'] = get_docker_secret(service_name_env + '_ENV_POSTGRES_USER', 'postgres')
          database['password'] = get_docker_secret(service_name_env + '_ENV_POSTGRES_PASSWORD')
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




if __name__ == '__main__':
    service = ServiceRun()

    service.backup_duplicity_ftp(os.getenv('TARGET_BACKEND'), os.getenv('TARGET_PATH', "/backup/postgres"),os.getenv('BK_FULL_FREQ', "7D"), os.getenv('BK_KEEP_FULL', "3"), os.getenv('BK_KEEP_FULL_CHAIN', "1"), os.getenv('VOLUME_SIZE', "25"), True)
    service.backup_postgres()
    service.backup_duplicity_ftp(os.getenv('TARGET_BACKEND'), os.getenv('TARGET_PATH', "/backup/postgres"),os.getenv('BK_FULL_FREQ', "7D"), os.getenv('BK_KEEP_FULL', "3"), os.getenv('BK_KEEP_FULL_CHAIN', "1"), os.getenv('VOLUME_SIZE', "25"))
