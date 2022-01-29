#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import yaml
import re
from generators.template_file.template import FIRST_FULL_SYNC_SHELL_FORMAT, FIRST_FULL_SYNC_SHELL_FORMAT_V2, DAILY_APPEND_SYNC_SHELL_FORMAT, \
    AZKABAN_JOB_FORMAT, SPARK_JOB_FORMAT, \
    CLEAR_YARN_ZEPPELIN_JOBS, SPARK_JOB_FORMAT_ONLINE
from generators.template_file import templateUpdate, templateOverwriteV2, templateOverwriteV3,templateSubmitSql, templateSubmitPy,templatesnapshot
from generators import conf
import requests
import subprocess
import shutil

# 下线文件
discard_flow_list_online=[]
ana_base_path = "/data/1/analystic_py37/"
her_base_path = "/data/1/Hermes/"

class Generate():
    def __init__(self):
        # self.her_path = os.getenv("Hermes")
        self.her_path = '/Users/apple/Public/hunliji/code/py/pro/data_warehouse'
        # self.ana_path = os.getenv("ANALHOME")
        # self.ana_path = '/home/hunliji/analystic_py37/'
        # self.ana_path = '/data/analystic_py37/'
        self.ana_path = '/Users/apple/Public/hunliji/code/py/pro/data_warehouse'
        self.flows = []
        self.flows_biz = []
        self.build_path = '/Users/apple/Public/hunliji/code/py/pro/data_warehouse' + '/build'
        self.hermes = 'Hermes'
        self.biz_project_name = 'Biz'

        if env == 'online':
            self.project_name = 'Hermes_analystic_daily'
        else:
            self.project_name = 'Hermes'

    def zip_job(self, project_name):
        with open(os.devnull, 'w') as devnull:
            s='rm -rf '+self.her_path+'/' + project_name + '.zip'
            subprocess.check_call(s, shell=True, stdout=devnull)

        with open(os.devnull, 'w') as devnull:
            s='cd ' + self.build_path + ';zip -r '+self.her_path+'/' + project_name + '.zip *'
            subprocess.check_call(s, shell=True,
                                  stdout=devnull)

    # azkaban任务打包上传到AzkabanWeb
    def upload_project(self, project_name):
        session_id = self.get_session_id()
        payload = {
            "session.id": session_id,
            "ajax": "upload",
            "project": project_name,
        }
        file = {'file': (project_name + '.zip',
                         open(self.her_path+'/' + project_name + '.zip', 'rb'),
                         'application/zip')}
        r = requests.post(AZKABAN_URL + '/manager', data=payload, files=file, verify=False)
        print (r.text)
        response = r.json()
        if response.get("error") is None:
            print("upload project %s succeeded." % project_name)
        else:
            raise Exception("upload failed. error=%s" % (response.get("error")))

    # 创建项目
    def create_project(self, project_name):
        session_id = self.get_session_id()

        payload = {
            "session.id": session_id,
            "action": "create",
            "name": project_name,
            "description": "%s Project of Hunliji Data Center" % project_name
        }
        print(AZKABAN_URL + '/manager?action=create')
        r = requests.post(AZKABAN_URL + '/manager?action=create', params=payload, verify=False)
        print(AZKABAN_URL)
        print (r.text)
        response = r.json()
        if response.get("error") is None:
            print("create project %s succeeded." % project_name)
        else:
            raise Exception("upload failed. error=%s" % (response.get("error")))

    def get_session_id(self):
        # payload = {
        #     "action": "login",
        #     "username": AZKABAN_USER,
        #     "password": AZKABAN_PWD
        # }
        # print(payload)
        # print(AZKABAN_URL + '/manager')
        # r = requests.post(AZKABAN_URL + '/manager', params=payload, verify=False)

        postdata = {'username': 'azkaban', 'password': 'azkaban'}
        login_url = 'http://150.158.58.149:8081' + '?action=login'
        r = requests.post(login_url, postdata, verify=False)


        response = r.json()

        if response.get("error") is not None:
            raise Exception("login failed. error=%s" % (response.get("error")))

        session_id = r.json()['session.id']
        return session_id

    def parse_yaml(self):
        yaml_path = self.her_path+"/config"
        for root, dirs, files in os.walk(yaml_path):
            # 遍历文件
            for f in files:
                yaml_path = os.path.join(root, f)
                print(yaml_path)
                self.parse_config_yaml(yaml_path)
            # # 遍历所有的文件夹
            # for d in dirs:
            #     print(os.path.join(root, d))

    def parse_config_yaml(self, yaml_path):
        folder = os.path.exists(self.build_path)
        if not folder:
            os.system("mkdir -p " + self.build_path)
        with open(yaml_path, 'r') as f:
            # sqoop_config = yaml.load(f)
            sqoop_config = yaml.safe_load(f)
        self.write_sqoop(sqoop_config)

    def write_sqoop(self, sqoop_config):

        source = sqoop_config['source']
        hive_database = sqoop_config['hive_database']
        for db, databases in source.items():
            source = db
            host = DB_CONFIG[source]['host']
            port = DB_CONFIG[source]['port']
            username = DB_CONFIG[source]['username']
            password = DB_CONFIG[source]['password']
            for database, tables in databases.items():
                source_database = database
                target_database = database
                for table in tables['tables']:
                    source_table = table['table_name']
                    mode = table['mode']
                    biz_hive_table = '__'.join([source, source_database, source_table])
                    if biz_hive_table in discard_flow_list_online and env == "online":
                        continue
                    if mode == "overwrite" and TMP_BIZ == 0:
                        build_path = self.build_path + '/overwrite/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        split_column = table['split_column']
                        check_column = table['check_column']
                        if check_column != 'none':
                            job_sh = FIRST_FULL_SYNC_SHELL_FORMAT.format(host=host,
                                                                         port=port,
                                                                         username=username,
                                                                         password=password,
                                                                         hive_database=hive_database,
                                                                         biz_hive_table=biz_hive_table,
                                                                         source=source,
                                                                         source_database=source_database,
                                                                         target_database=target_database,
                                                                         source_table=source_table,
                                                                         split_column=split_column,
                                                                         check_column=check_column,
                                                                         auth=env_var["auth"])
                            with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                                f.write(job_sh)
                            with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                                f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                                  retries=3,
                                                                  command="sh %s.sh" % biz_hive_table,
                                                                  dependencies=""))
                        else:
                            job_sh = FIRST_FULL_SYNC_SHELL_FORMAT_V2.format(host=host,
                                                                            port=port,
                                                                            username=username,
                                                                            password=password,
                                                                            hive_database=hive_database,
                                                                            biz_hive_table=biz_hive_table,
                                                                            source=source,
                                                                            source_database=source_database,
                                                                            target_database=target_database,
                                                                            source_table=source_table,
                                                                            split_column=split_column,
                                                                            auth=env_var["auth"])
                            with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                                f.write(job_sh)
                            with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                                f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                                  retries=3,
                                                                  command="sh %s.sh" % biz_hive_table,
                                                                  dependencies=""))
                    elif mode == "overwriteV3" and TMP_BIZ == 0:
                        columns = table['columns']
                        split_column = table['split_column']
                        num_mappers = table['num_mappers']
                        hive_table_suffix = table['hive_table_suffix']
                        status = table['status']
                        if status == 'off':
                            continue
                        biz_hive_table = biz_hive_table + hive_table_suffix
                        job_sh = templateOverwriteV3.OVERWRITE_FORMAT.format(host=host,
                                                   port=port,
                                                   username=username,
                                                   password=password,
                                                   hive_database=hive_database,
                                                   biz_hive_table=biz_hive_table,
                                                   source=source,
                                                   source_database=source_database,
                                                   target_database=target_database,
                                                   source_table=source_table,
                                                   split_column=split_column,
                                                   columns=columns,
                                                   env=env,
                                                   num_mappers=num_mappers)
                        build_path = self.build_path + '/overwriteV3/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                            f.write(job_sh + templateOverwriteV3.OVERWRITE_FORMAT_ORIGIN)
                        with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                            f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                              retries=3,
                                                              command="sh %s.sh" % biz_hive_table,
                                                              dependencies=""))
                        self.flows_biz.append(biz_hive_table)
                    elif mode == "snapshot" and TMP_BIZ == 0:
                        columns = table['columns']
                        split_column = table['split_column']
                        num_mappers = table['num_mappers']
                        hive_table_suffix = table['hive_table_suffix']
                        status = table['status']
                        if status == 'off':
                            continue
                        biz_hive_table = biz_hive_table + hive_table_suffix
                        job_sh = templatesnapshot.FORMAT.format(host=host,
                                                                             port=port,
                                                                             username=username,
                                                                             password=password,
                                                                             hive_database=hive_database,
                                                                             biz_hive_table=biz_hive_table,
                                                                             source=source,
                                                                             source_database=source_database,
                                                                             target_database=target_database,
                                                                             source_table=source_table,
                                                                             split_column=split_column,
                                                                             columns=columns,
                                                                             env=env,
                                                                             num_mappers=num_mappers)
                        build_path = self.build_path + '/snapshot/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                            f.write(job_sh + templatesnapshot.ORIGIN)
                        with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                            f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                              retries=3,
                                                              command="sh %s.sh append-all" % biz_hive_table,
                                                              dependencies=""))
                        self.flows_biz.append(biz_hive_table)
                    elif mode == "overwriteV2" and TMP_BIZ == 0:
                        columns = table['columns']
                        create_hivetable_columns = table['create_hivetable_columns']
                        split_column = table['split_column']
                        hive_table_suffix = table['hive_table_suffix']
                        num_mappers = table['num_mappers']
                        status = table['status']
                        if status == 'off':
                            continue
                        biz_hive_table = biz_hive_table + hive_table_suffix
                        job_sh = templateOverwriteV2.OVERWRITE_FORMAT.format(host=host,
                                                                     port=port,
                                                                     username=username,
                                                                     password=password,
                                                                     hive_database=hive_database,
                                                                     biz_hive_table=biz_hive_table,
                                                                     source=source,
                                                                     source_database=source_database,
                                                                     target_database=target_database,
                                                                     source_table=source_table,
                                                                     columns=columns,
                                                                     create_hivetable_columns=create_hivetable_columns,
                                                                     split_column=split_column,
                                                                     env=env,
                                                                     num_mappers=num_mappers)
                        build_path = self.build_path + '/overwriteV2/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                            f.write(job_sh + templateOverwriteV2.OVERWRITE_FORMAT_ORIGIN)
                        with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                            f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                              retries=3,
                                                              command="sh %s.sh overwrite" % biz_hive_table,
                                                              dependencies=""))
                        self.flows_biz.append(biz_hive_table)
                    elif mode == "update":
                        columns = table['columns']
                        create_hivetable_columns = table['create_hivetable_columns']
                        split_column = table['split_column']
                        if('hive_table_suffix' in table.keys()):
                            hive_table_suffix=table['hive_table_suffix']
                        else:
                            hive_table_suffix=''
                        num_mappers = table['num_mappers']
                        status = table['status']
                        if('create_column' in table.keys()):
                            create_column=table['create_column']
                        else:
                            create_column='created_at'
                        if('update_column' in table.keys()):
                            update_column=table['update_column']
                        else:
                            update_column='updated_at'
                        if('update_need_end' in table.keys()):
                            update_need_end=table['update_need_end']
                        else:
                            update_need_end='Y'

                        if status == 'off':
                            continue
                        biz_hive_table = biz_hive_table + hive_table_suffix
                        job_sh = templateUpdate.UPDATE_FORMAT.format(host=host,
                                                      port=port,
                                                      username=username,
                                                      password=password,
                                                      hive_database=hive_database,
                                                      biz_hive_table=biz_hive_table,
                                                      source=source,
                                                      source_database=source_database,
                                                      target_database=target_database,
                                                      source_table=source_table,
                                                      columns=columns,
                                                      create_hivetable_columns=create_hivetable_columns,
                                                      split_column=split_column,
                                                      env=env,
                                                      num_mappers=num_mappers,
                                                        create_column=create_column,
                                                                     update_column=update_column,
                                                                     update_need_end=update_need_end)
                        build_path = self.build_path + '/update/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                            f.write(job_sh + templateUpdate.UPDATE_FORMAT_ORIGIN)
                        with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                            f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                              retries=3,
                                                              command="sh %s.sh update" % biz_hive_table,
                                                              dependencies=""))
                        self.flows_biz.append(biz_hive_table)

                    elif mode == "append":
                        split_column = table['split_column']
                        check_column = table['check_column']
                        job_sh = DAILY_APPEND_SYNC_SHELL_FORMAT.format(host=host,
                                                                       port=port,
                                                                       username=username,
                                                                       password=password,
                                                                       hive_database=hive_database,
                                                                       biz_hive_table=biz_hive_table,
                                                                       source=source,
                                                                       source_database=source_database,
                                                                       target_database=target_database,
                                                                       source_table=source_table,
                                                                       split_column=split_column,
                                                                       check_column=check_column,
                                                                       auth=env_var["auth"],
                                                                       env=env)
                        build_path = self.build_path + '/append/'
                        folder = os.path.exists(build_path)
                        if not folder:
                            os.system("mkdir -p " + build_path)
                        with open(os.path.join(build_path, biz_hive_table + '.sh'), 'w') as f:
                            f.write(job_sh)
                        with open(os.path.join(build_path, biz_hive_table + '.job'), 'w') as f:
                            f.write(AZKABAN_JOB_FORMAT.format(job_name=biz_hive_table,
                                                              retries=3,
                                                              command="sh %s.sh" % biz_hive_table,
                                                              dependencies=""))

                        self.flows_biz.append(biz_hive_table)
                    self.flows.append(biz_hive_table)
    #                 # print job_sh

    def parse_etl_jobs(self):
        etl_jobs_path = self.her_path
        database = ['temp', 'src', 'dw','ods', 'dm','dwd','dws','dwt']
        build_path = self.build_path + '/etl_jobs/'
        folder = os.path.exists(build_path)
        if not folder:
            os.system("mkdir -p " + build_path)
        for db in database:
            job_path = os.path.join(etl_jobs_path, db)
            for py_file in [file for file in os.listdir(job_path) if
                            file.endswith('.py')  and file != '__init__.py']:
                dependencies = set()
                job_name = db + '__' + py_file.split('.')[0]

                if env == 'offline' and job_name in ['dw__user_info_v2','dw__user_info_v3']:
                    continue
                if env == 'online' and job_name not in ['dm__bi_haicaomingpian_new', 'dm__bi_photo_merchant_model'] and (
                        job_name.startswith('dm__bi') or job_name.startswith('dm__neo4j') or job_name.startswith(
                        'dm__mysql')):
                    continue

                text = open(os.path.join(job_path, py_file)).read()
                mod = u'((?<=\s)(biz|dm|src|dw|dwd|dws|dwt)\.\S+(?=\s))'
                pattern = re.compile(mod)
                results = pattern.findall(text)
                for ret in results:
                    job = ret[0].split('.')[1].strip(')') if ret[0].__contains__('biz') else ret[0].replace('.', '__').strip(')')
                    dependencies.add(job)
                if job_name in dependencies:
                    dependencies.remove(job_name)
                dependencies = ",".join(dependencies)

                if env == 'offline':
                    command = SPARK_JOB_FORMAT.format(database=db,
                                                      job_name=job_name.split('__')[1],
                                                      her_base_path=her_base_path)
                else:
                    command = SPARK_JOB_FORMAT_ONLINE.format(database=db,
                                                             job_name=job_name.split('__')[1],
                                                             her_base_path=her_base_path)
                with open(os.path.join(build_path, job_name + '.job'), 'w') as f:
                    f.write(AZKABAN_JOB_FORMAT.format(job_name=job_name,
                                                      retries=3,
                                                      command=command,
                                                      dependencies=dependencies))
                self.flows.append(job_name)

    def parse_etl_jobs_v2(self,database,path):
        # etl_jobs_path = self.her_path + '/apps'+'/etl_jobs_v2'
        # database = ['test']
        # build_path = self.build_path + '/etl_jobs_v2/'
        etl_jobs_path = self.her_path +path
        database = database
        build_path = self.build_path + path
        folder = os.path.exists(build_path)
        if not folder:
            os.system("mkdir -p " + build_path)
        for db in database:
            job_path = os.path.join(etl_jobs_path, db)
            for py_file in [file for file in os.listdir(job_path) if
                            (file.endswith('.py') or file.endswith('.sql')) and file != '__init__.py']:
                dependencies = set()
                job_name = db + '__' + py_file.split('.')[0]
                mode=py_file.split('.')[1]

                if env == 'offline' and job_name in ['dw__user_info_v2','dw__user_info_v3']:
                    continue
                if env == 'online' and job_name not in ['dm__bi_haicaomingpian_new', 'dm__bi_photo_merchant_model'] and (
                        job_name.startswith('dm__bi') or job_name.startswith('dm__neo4j') or job_name.startswith(
                    'dm__mysql')):
                    continue
                text = open(os.path.join(job_path, py_file)).read()
                mod = u'((?<=\s)(biz|dm|src|dw|dwd|ods|dws|dwt)\.\S+(?=\s))'
                pattern = re.compile(mod)
                results = pattern.findall(text)
                for ret in results:
                    job = ret[0].split('.')[1].strip(')') if ret[0].__contains__('biz') else ret[0].replace('.', '__').strip(')')
                    dependencies.add(job)
                if job_name in dependencies:
                    dependencies.remove(job_name)
                dependencies = ",".join(dependencies)

                resource_mod = '(?<=resource_begin)[\s\S]*?(?=resource_end)'
                resource_pattern = re.compile(resource_mod)
                resource = resource_pattern.findall(text)[0]
                driver_memory = re.search(r".*@driver_memory:.*", resource)
                driver_memory = re.sub('\s', '', driver_memory.group()).split("@driver_memory:")[1] if driver_memory is not None else ""
                if driver_memory=="":
                    driver_memory="2g"
                # print (driver_memory)
                num_executors = re.search(r".*@num_executors:.*", resource)
                num_executors = re.sub('\s', '', num_executors.group()).split("@num_executors:")[1] if num_executors is not None else ""
                if num_executors=="":
                    num_executors="7"
                # print (num_executors)
                executor_memory = re.search(r".*@executor_memory:.*", resource)
                executor_memory = re.sub('\s', '', executor_memory.group()).split("@executor_memory:")[1] if executor_memory is not None else ""
                if executor_memory=="":
                    executor_memory="4g"
                # print (executor_memory)
                executor_cores = re.search(r".*@executor_cores:.*", resource)
                executor_cores = re.sub('\s', '', executor_cores.group()).split("@executor_cores:")[1] if executor_cores is not None else ""
                if executor_cores=="":
                    executor_cores="1"
                # print (executor_cores)
                deploy_mode = re.search(r".*@deploy_mode:.*", resource)
                deploy_mode = re.sub('\s', '', deploy_mode.group()).split("@deploy_mode:")[1] if deploy_mode is not None else ""
                if deploy_mode=="":
                    deploy_mode="cluster"
                # print (deploy_mode)

                conf_mod = '(?<=conf_begin)[\s\S]*?(?=conf_end)'
                conf_pattern = re.compile(conf_mod)
                if len(conf_pattern.findall(text)) > 0:
                    conf = conf_pattern.findall(text)[0]
                    state = re.search(r".*@state:.*", conf)
                    state = re.sub('\s', '', state.group()).split("@state:")[1] if state is not None else ""
                    if state=="":
                        state="on"
                    # print (state)
                    if state == "off":
                        continue

                if mode == 'sql':
                    sql_mod = '(?<=sql_begin)[\s\S]*?(?=sql_end)'
                    sql_pattern = re.compile(sql_mod)
                    sql = sql_pattern.findall(text)[0]

                    job_sh = templateSubmitSql.FORMAT.format(sql=sql,
                                                             job_name=job_name.split('__')[1],
                                                             driver_memory=driver_memory,
                                                             num_executors=num_executors,
                                                             executor_memory=executor_memory,
                                                             executor_cores=executor_cores)
                    if env == 'offline':
                        job_sh = job_sh + templateSubmitSql.OFFLINE_ORIGIN
                    elif env == 'online':
                        job_sh = job_sh + templateSubmitSql.ONLINE_ORIGIN
                    with open(os.path.join(build_path, job_name + '.sh'), 'w') as f:
                        f.write(job_sh)
                    with open(os.path.join(build_path, job_name + '.job'), 'w') as f:
                        f.write(AZKABAN_JOB_FORMAT.format(job_name=job_name,
                                                          retries=3,
                                                          command="sh %s.sh" % job_name,
                                                          dependencies=dependencies))
                elif mode == 'py':
                    file_path_temp=her_base_path+db+'/'+job_name.split('__')[1]+'.py'
                    job_sh = templateSubmitPy.FORMAT.format(job_name=job_name.split('__')[1],
                                                            file_path_temp=file_path_temp,
                                                            database=db,
                                                            driver_memory=driver_memory,
                                                            num_executors=num_executors,
                                                            executor_memory=executor_memory,
                                                            executor_cores=executor_cores,
                                                            deploy_mode=deploy_mode)
                    if env == 'offline':
                        job_sh = job_sh + templateSubmitPy.OFFLINE_ORIGIN
                    elif env == 'online':
                        job_sh = job_sh + templateSubmitPy.ONLINE_ORIGIN
                    with open(os.path.join(build_path, job_name + '.sh'), 'w') as f:
                        f.write(job_sh)
                    with open(os.path.join(build_path, job_name + '.job'), 'w') as f:
                        f.write(AZKABAN_JOB_FORMAT.format(job_name=job_name,
                                                          retries=3,
                                                          command="sh %s.sh" % job_name,
                                                          dependencies=dependencies))
                self.flows.append(job_name)

    # 数据分析任务流
    def parse_anal_jobs(self):
        anal_jobs_path = self.ana_path
        print("anal_jobs_path: %s" % anal_jobs_path)
        database = ['apps', 'pyapp', 'biapp']
        # \n.* 提高效率
        mod = u'\\s.*?re_begin\n.*\n.*\n.*\n.*\n.*\n.*\n.*\n.*\n.*\n.*\n.*?re_end'
        pattern = re.compile(mod)
        for db in database:
            # 递归目录
            job_path = os.path.join(anal_jobs_path, db)

            for base, k, py_list in os.walk(job_path):
                for py in py_list:
                    if py.endswith(".py") and py != "__init__.py":
                        text = open(os.path.join(base, py)).read()
                        results = pattern.findall(text)
                        if len(results) > 0:
                            re_file_t = re.search(r".*@In:.*", results[0])
                            re_file = re.sub('\s', '', re_file_t.group()).split("@In:")[
                                1] if re_file_t is not None else ""
                            re_kind_t = re.search(r".*@Kind:.*", results[0])
                            re_kind = re.sub("\s", "", re_kind_t.group()).split("@Kind:")[
                                1] if re_kind_t is not None else ""
                            re_arg_t = re.search(r".*@Arg:.*", results[0])
                            re_arg = re.sub("\s", "", re_arg_t.group()).split("@Arg:")[
                                1] if re_arg_t is not None else ""
                            re_file_arr = re_file.split("|")
                            re_kind_arr = re_kind.split("|")
                            if len(re_file_arr)!= len(re_kind_arr):
                                raise Exception("In和Kind数量不统一")

                            for i in range(0, len(re_file_arr)):
                                args = re_arg
                                if re_kind_arr[i] == '':
                                    continue
                                if re_kind_arr[i] != 'daily':
                                    continue
                                re_file = re_file_arr[i]
                                re_kind = re_kind_arr[i]
                                pattern_test = re.compile(r'test_')
                                result_test = pattern_test.findall(re_kind)
                                if len(result_test) > 0:
                                    args = args + " " + 'db_test'
                                job_name = py.split(".py")[0]
                                command = "python {base_path}/{file} {arg}".format(
                                    base_path=ana_base_path + base.split("analystic_py37/")[1], file=py, arg=args)
                                folder = os.path.exists(self.build_path + "/" + re_kind)
                                if not folder:
                                    os.system("mkdir -p " + self.build_path + "/" + re_kind)
                                with open(os.path.join(self.build_path + "/" + re_kind, job_name + '.job'), 'w') as f:
                                    f.write(AZKABAN_JOB_FORMAT.format(job_name=job_name,
                                                                      retries=3,
                                                                      command=command,
                                                                      dependencies=re_file))
                                self.flows.append(job_name)

    # 生成任务依赖
    def build_dependencies(self):
        self.parse_yaml()
        # self.parse_etl_jobs()
        database = ['dwd','dws','dwt','dm','src','ods']
        path='/app'
        self.parse_etl_jobs_v2(database,path)
        # database = []
        # path='/etl_jobs_v3'
        # self.parse_etl_jobs_v2(database,path)

        if env == 'online':
            with open(os.path.join(self.build_path, self.hermes + '.job'), 'w') as f:
                f.write(AZKABAN_JOB_FORMAT.format(job_name=self.project_name,
                                                  retries=2,
                                                  command="echo Hermes Down",
                                                  dependencies=",".join(self.flows)))

            self.parse_anal_jobs()
            with open(os.path.join(self.build_path, self.project_name + '.job'), 'w') as f:
                f.write(AZKABAN_JOB_FORMAT.format(job_name=self.project_name,
                                                  retries=2,
                                                  command="python %sgenerators/robot_succeed_notify.py %s" %(her_base_path,self.project_name),
                                                  dependencies=",".join(self.flows)))
        else:
            with open(os.path.join(self.build_path, self.project_name + '.job'), 'w') as f:
                f.write(AZKABAN_JOB_FORMAT.format(job_name=self.project_name,
                                                  retries=2,
                                                  command="echo Hermes Down",
                                                  dependencies=",".join(self.flows)))

        # add yarn zeppelin-jobs clear
        # with open(os.path.join(self.build_path, "clear_yarn_zeppelin_jobs" + '.py'), 'w') as f:
        #     f.write(CLEAR_YARN_ZEPPELIN_JOBS)
        # with open(os.path.join(self.build_path, "clear_yarn_zeppelin_jobs" + '.job'), 'w') as f:
        #     f.write(AZKABAN_JOB_FORMAT.format(job_name="clear_yarn_zeppelin_jobs",
        #                                       retries=2,
        #                                       command="python %s.py" % "clear_yarn_zeppelin_jobs",
        #                                       dependencies=""))

        self.zip_job(self.project_name)

    def build_dependencies_biz(self):
        self.parse_yaml()
        print (self.flows_biz.__len__())
        print (TMP_BIZ)

        # 单独生成任务
        # 只针对append的表单独成job流
        with open(os.path.join(self.build_path, self.biz_project_name + '.job'), 'w') as f:
            f.write(AZKABAN_JOB_FORMAT.format(job_name=self.biz_project_name,
                                              retries=2,
                                              command="echo Biz Finish",
                                              dependencies=",".join(self.flows_biz)))
        self.zip_job(self.biz_project_name)

    def run(self):
        if os.path.exists(self.build_path):
            shutil.rmtree(self.build_path)

        self.build_dependencies()
        # self.build_dependencies_biz()

        # exit()
        self.create_project(self.project_name)
        self.upload_project(self.project_name)

    def yaml(self):
        if os.path.exists(self.build_path):
            shutil.rmtree(self.build_path)
        self.build_dependencies()

    def deploy_biz_project(self):
        if os.path.exists(self.build_path):
            shutil.rmtree(self.build_path)
        self.create_project(self.biz_project_name)
        self.build_dependencies_biz()
        self.upload_project(self.biz_project_name)



if __name__ == '__main__':
    args = sys.argv[1:]

    TMP_BIZ = 0
    env = "online"
    function = ""
    if len(sys.argv[1:]) > 0:
        assert sys.argv[1] in ('online', 'offline')
        env = sys.argv[1]
    g = Generate()
    if len(sys.argv[1:]) > 1:
        assert sys.argv[2] in ('yaml')
        function = sys.argv[2]

    env_var = conf[env]
    AZKABAN_URL = env_var["azkaban"]["AZKABAN_URL"]
    AZKABAN_USER = env_var["azkaban"]["AZKABAN_USER"]
    AZKABAN_PWD = env_var["azkaban"]["AZKABAN_PWD"]
    DB_CONFIG = env_var["db"]

    if function == 'yaml':
        g.yaml()
        exit()
    g.run()
