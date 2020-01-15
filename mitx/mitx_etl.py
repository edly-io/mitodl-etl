#!/usr/bin/env python

"""
Script to run SQL queries on mitx residential and upload them to an S3 bucket.
"""

import csv
import json
import os
import subprocess
import sys
import tarfile
from datetime import datetime
import logging

try:
    import requests
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text
except ImportError as err:
    print("Failed to import module: ", err)
    sys.exit("Make sure to install logbook, requests and sqlalchemy")

datetime = datetime.now()
date_suffix = datetime.strftime('%Y%m%d')
dir_path = os.path.dirname(os.path.realpath(__file__))

# Read settings_file
try:
    settings = json.load(open(os.path.join(dir_path, './settings.json')))
except IOError:
    sys.exit("[-] Failed to read settings file")

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Set some needed variables
mysql_creds_user = settings['MySQL']['user']
mysql_creds_pass = settings['MySQL']['pass']
mysql_host = settings['MySQL']['host']
mysql_db = settings['MySQL']['db']
course_ids = []
exported_courses_folder = settings['Paths']['courses'] + '/' + date_suffix + '/'
daily_folder = settings['Paths']['csv_folder'] + '/' + date_suffix + '/'

# List of db queries
query_dict = {
    'users_query': {'command': 'select auth_user.id, auth_user.username, auth_user.first_name, auth_user.last_name, auth_user.email, auth_user.is_staff, auth_user.is_active, auth_user.is_superuser, auth_user.last_login, auth_user.date_joined from auth_user inner join student_courseenrollment on student_courseenrollment.user_id = auth_user.id and student_courseenrollment.course_id = :course_id', 'fieldnames':  ['id', 'username', 'first_name', 'last_name', 'email', 'is_staff', 'is_active', 'is_superuser', 'last_login', 'date_joined']},
    'studentmodule_query': {'command': 'select id, module_type, module_id, student_id, state, grade, created, modified, max_grade, done, course_id from courseware_studentmodule where course_id= :course_id', 'fieldnames': ['id', 'module_type', 'module_id', 'student_id', 'state', 'grade', 'created', 'modified', 'max_grade', 'done', 'course_id']},
    'enrollment_query': {'command': 'select id, user_id, course_id, created, is_active, mode  from student_courseenrollment where course_id= :course_id', 'fieldnames': ['id', 'user_id', 'course_id', 'created', 'is_active', 'mode']},
    'role_query': {'command': 'select id,user_id,org,course_id,role from student_courseaccessrole where course_id= :course_id', 'fieldnames': ['id', 'user_id', 'org', 'course_id', 'role']}
}

def verify_and_create_required_folders(csv_folder, courses):
    """
    Check whether the folder that will contain csv query files exists

    Args:
      csv_folder (str): The path of the csv folder.

    Returns:
      If folder exists return None, and if not, logs error and exit.
    """
    if not os.path.exists(daily_folder):
        os.makedirs(daily_folder)
        logger.info("csv folder(s) created")

    if not os.path.exists(exported_courses_folder):
        os.makedirs(exported_courses_folder)
        logger.info("exported_courses_folder created")
     
def export_all_courses(exported_courses_folder):
    """
    Export all courses into specified folder

    Args:
      exported_courses_folder (str): The path of folder to export courses to.

    """
    try:
        course_list = subprocess.Popen(
            ['/edx/bin/python.edxapp',
             '/edx/app/edxapp/edx-platform/manage.py',
             'cms', '--settings', 'production',
             'dump_course_ids'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = course_list.communicate()
        for course_id in out.splitlines():
            course_id = course_id.decode('utf-8')
            logger.info("exporting course %s", course_id)
            export_course = subprocess.Popen(
                ['/edx/bin/python.edxapp',
                 '/edx/app/edxapp/edx-platform/manage.py',
                 'cms', '--settings', 'production',
                 'export_olx', course_id, '--output',
                 '{0}/{1}.tar.gz'.format(exported_courses_folder,
                                         course_id)])
            out, err = export_course.communicate()
    except ValueError as err:
            logger.error(
                "The following error was encountered when exporting courses: ",
                err)

def tar_exported_courses(exported_courses_folder):
    """
    Tar exported course folders and store them in daily_folder

    Args:
      exported_courses_folder (str): The path of folder to export courses to.
    """
    try:
        with tarfile.open(daily_folder + 'exported_courses_' + date_suffix + '.tar.gz', 'w:gz') as tar:
            tar.add(exported_courses_folder, arcname=os.path.sep)
    except tarfile.TarError as err:
        logger.error("The following error was encountered when compressing exported courses: ", err)

def get_course_ids():
    """
    Get a list of course ids that is necessary for the rest of the
    functions to work.
    """
    global course_ids
    dump_course_ids = subprocess.Popen(['/edx/bin/python.edxapp',
                                        '/edx/app/edxapp/edx-platform/manage.py',
                                        'lms', '--settings', 'production',
                                        'dump_course_ids'], stdout=subprocess.PIPE)
    course_ids = dump_course_ids.communicate()[0].split()
    return course_ids

def add_csv_header():
    """
    Create csv files and add header to each based on
    fieldnames in query_dict
    """
    for key, value in query_dict.items():
        with open(daily_folder + str(key) + '.csv', 'w+', encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=value['fieldnames'])
            writer.writeheader()

def mysql_query(course_ids):
    engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'
                           .format(mysql_creds_user, mysql_creds_pass, mysql_host, mysql_db))
    connection = engine.connect()
    for course_id in course_ids:
        for key, value in query_dict.items():
            query_text = text(value['command'])
            query = connection.execute(query_text, course_id=course_id.decode('utf8'))
            write_csv(query, key)

def write_csv(query, key):
    with open(daily_folder + str(key) + '.csv', 'a', encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in query:
            writer.writerow(row)

def main():
    verify_and_create_required_folders(settings['Paths']['csv_folder'],
                                       settings['Paths']['courses'])
    export_all_courses(exported_courses_folder)
    tar_exported_courses(exported_courses_folder)
    add_csv_header()
    get_course_ids()
    mysql_query(course_ids)

if __name__ == "__main__":
    main()
