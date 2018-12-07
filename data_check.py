import sys
import time
import os
import csv
import sqlite3
import re
import random
import datetime


class MembershipFields:
    def __init__(self):
        pass

    def get_fieldnames(self, version):
        fields = []
        if version == 'IA':
            fields = ['account', 'holderfname', 'holderbname',
                      'street1', 'street2', 'city', 'state', 'postalcode', 'fbname',
                      'fbstreet1', 'fbcity', 'dues', 'notice', 'Hay', 'CattleFeeders',
                      'DairyMature', 'FinishingHogs', 'Goats', 'holderworkphone',
                      'AH_FFA_Member']
        if version == 'SD':
            fields = ['account', 'holderfname', 'holderlname', 'holderbname',
                      'street1', 'street2', 'city', 'state', 'postalcode',
                      'sgender', 'fbname', 'dues', 'expireplus1', 'notice',
                      'GrossFarmIncome', 'irrigate', 'Corn', 'Honey',
                      'dep1fullname', 'classname', 'duesyear']
        if version == 'NM':
            fields = ['account', 'holderfname', 'holderlname', 'holderbname',
                      'street1', 'street2', 'city', 'state', 'postalcode',
                      'holdertownship', 'holderemail', 'holderphone', 'holderbdate',
                      'holdergender', 'fbname', 'dues', 'expireplus1', 'notice']
        if version == 'UT':
            # fields = ['account']
            fields = ['account', 'holderfname', 'holderlname', 'holderbname', 'street1',
                      'street2', 'city', 'state', 'postalcode', 'holderemail', 'sbdate',
                      'sgender', 'fbname', 'dues', 'expireplus1', 'notice', 'Beef',
                      'Dairy', 'businessaccount', 'holderworkphone']

        return fields


class MembershipSource:
    def __init__(self, param):
        self.parameters = param
        self.filename = ''
        self.proc_dir = ''

    def get_processing_dir(self):
        proc_dir = [f for f in os.listdir(os.path.curdir)
                    if f[0:5] == str(self.parameters['job_number'])]

        self.proc_dir = os.path.join(os.path.curdir, proc_dir[0])

    def get_filename(self, state):
        if state == 'IA':
            first = re.compile('(welcome)[\S\s]*.txt')
            second = re.compile('(duesnotice)[\S\s]*.txt')
        else:
            first = re.compile('(duesnotice)[\S\s]*.txt')
            second = re.compile('(welcome)[\S\s]*.txt')

        # First, Paid Welcome
        if self.parameters['split'] == 0:
            src_fle = [f for f in os.listdir(self.proc_dir) if first.search(f)][0]
        # Second
        if self.parameters['split'] == 1:
            src_fle = [f for f in os.listdir(self.proc_dir) if second.search(f)][0]

        if not os.path.isfile(os.path.join(self.proc_dir, src_fle)):
            print("Error, Invalid search")
            time.sleep(3)
            self.filename = None
        else:
            self.filename = src_fle

    def import_data(self):

        with open(os.path.join(self.proc_dir, self.filename), 'r') as s:
            csvr = csv.DictReader(s, delimiter='\t')
            fieldnames = csvr.fieldnames
            db_fields = ' VARCHAR(100), '.join(fieldnames)

            sqldb = sqlite3.connect('source_data.db')
            c = sqldb.cursor()
            c.execute("DROP TABLE IF EXISTS source_data;")
            c.execute("VACUUM;")
            c.execute("CREATE TABLE source_data ({0});".format(db_fields))

            placeholders = ', '.join(['?'] * len(fieldnames))
            fields = ', '.join(fieldnames)

            for line in csvr:
                sql = "INSERT INTO source_data ({0}) VALUES ({1});".format(fields, placeholders)
                c.execute(sql, list(line.values()))

            sqldb.commit()
            sqldb.close()


class MembershipMerge:
    def __init__(self, param):
        self.parameters = param
        self.proc_dir = ''
        self.current_processing_file = ''
        self.current_processing_file_total_records = 0
        self.sample_size = 0
        self.sample_records = []
        self.split_1_files = []
        self.split_2_files = []
        self.process_state = ''

    def get_processing_dir(self):
        proc_dir = [f for f in os.listdir(os.path.curdir) 
                    if f[0:5] == str(self.parameters['job_number'])]

        self.process_state = proc_dir[0].split(" ")[1]
        self.proc_dir = os.path.join(os.path.curdir, proc_dir[0], 'DP')

    def get_split_1_files(self):
        if self.process_state == 'IA':
            print("split 1 for IA")
            srch = re.compile("[^(2nd)[\S\s]*.txt]|(1st)[\S\s]*.txt|(Paid)[\S\s]*.txt|(Welcome)[\S\s]*.txt")
            self.split_1_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])
        elif self.process_state == 'UT':
            print("split 1 for UT")
            srch = re.compile("[^(Welcome)[\S\s]*.txt]|[^(Paid)[\S\s]*.txt]|(1st)[\S\s]*.txt|(2nd)[\S\s]*.txt")
            self.split_1_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])
        elif self.process_state in ['SD', 'NM']:
            print("split 1 for other")
            srch = re.compile("[^(Welcome)[\S\s]*.txt]|(1st)[\S\s]*.txt|(Paid)[\S\s]*.txt|(2nd)[\S\s]*.txt")
            self.split_1_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])

    def get_split_2_files(self):
        if self.process_state == 'IA':
            print("split 2 for IA")
            srch = re.compile("(2nd)[\S\s]*.txt")
            self.split_2_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])
        elif self.process_state == 'UT':
            print("split 2 for UT")
            srch = re.compile("(Paid)[\S\s]*.txt|(Welcome)[\S\s]*.txt")
            self.split_2_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])
        elif self.process_state in ['SD', 'NM']:
            print("split 2 for other")
            srch = re.compile("(Welcome)[\S\s]*.txt")
            self.split_2_files = ([f for f in os.listdir(self.proc_dir) if srch.search(f)])

    def get_sample_size(self):
        # Get total record count
        with open(os.path.join(self.proc_dir, self.current_processing_file), 'r') as f:
            self.current_processing_file_total_records = (len(f.readlines())) - 1
            
        # Get total sample size, either 25 or 1% of total, whichever is more
        self.sample_size = (min(int(max(25.0, self.current_processing_file_total_records * .01)),
                                self.current_processing_file_total_records))
        
    def get_sample_records(self):
        # Returns a list of records to be used as samples
        self.sample_records = set(random.sample(range(1, self.current_processing_file_total_records), 
                                                (self.sample_size - 1)))

    def set_processing_file(self, file):
        self.current_processing_file = file
        self.get_sample_size()
        self.get_sample_records()


class HCMProcess:
    def __init__(self, param):
        self.week_number = param['week_number']
        self.data_source_dir = ''
        self.merge_source_dir = ''
        self.data_source_file = ''
        self.merge_source_file = ''
        self.compare_fields = ['Wellmark_ID', 'Member_First_Name', 'Member_Last_Name',
                               'Plan_Member_Addr1', 'Plan_Member_Addr2', 'Plan_Member_City',
                               'Plan_Member_State', 'Plan_Member_Zip', 'Account_Key',
                               'Group_Name', 'Group_Num', 'Billing_Unit']
        self.sample_records = []
        self.total_records = 0
        self.sample_size = 0

    def get_source_dirs(self):
        date_dir = re.compile("([0-9])*-([0-9])*-([0-9])*")
        proc_dir = [f for f in os.listdir(os.path.curdir) if date_dir.search(f)]

        dated_folders = {}
        for folder in proc_dir:
            dated_folders[folder] = datetime.datetime.strptime(folder, "%m-%d-%y")

        sorted_folders = sorted(dated_folders.items(), key=lambda kv: kv[1])

        self.merge_source_dir = os.path.join(os.curdir, 'Week {0}'.format(self.week_number))
        self.data_source_dir = os.path.join(os.curdir, sorted_folders[(self.week_number - 1)][0])

    def get_source_files(self):
        try:
            self.data_source_file = ([f for f in os.listdir(self.data_source_dir)
                                      if str(f[-3:]).upper() == 'TXT'])[0]
            self.merge_source_file = ([f for f in os.listdir(self.merge_source_dir)
                                      if str(f[-3:]).upper() == 'TXT'])[0]
        except IndexError:
            print('Error!! Source file not found')

    def get_sample_size(self):
        # Get total record count
        with open(os.path.join(self.merge_source_dir, self.merge_source_file), 'r') as f:
            self.total_records = (len(f.readlines())) - 1

        # Get total sample size, either 25 or 1% of total, whichever is more
        self.sample_size = (min(int(max(25.0, self.total_records * .01)),
                                self.total_records))

    def get_sample_records(self):
        # Returns a list of records to be used as samples
        self.sample_records = set(random.sample(range(1, self.total_records),
                                                (self.sample_size - 1)))

    def import_data(self):
        with open(os.path.join(self.data_source_dir, self.data_source_file), 'r') as s:
            csvr = csv.DictReader(s, delimiter=',')
            fieldnames = csvr.fieldnames
            fieldnames = list(map(lambda x: x.replace('#', ''), fieldnames))
            # print(fieldnames)
            db_fields = ' VARCHAR(100), '.join(fieldnames)

            sqldb = sqlite3.connect('source_data.db')
            c = sqldb.cursor()
            c.execute("DROP TABLE IF EXISTS source_data;")
            c.execute("VACUUM;")
            c.execute("CREATE TABLE source_data ({0});".format(db_fields))

            placeholders = ', '.join(['?'] * len(fieldnames))
            fields = ', '.join(fieldnames)

            for line in csvr:
                sql = "INSERT INTO source_data ({0}) VALUES ({1});".format(fields, placeholders)
                c.execute(sql, list(line.values()))

            c.execute("UPDATE source_data SET Plan_Member_Addr2 = REPLACE(Plan_Member_Addr2, '_', '') ;")
            sqldb.commit()
            sqldb.close()


class PregMerge:
    def __init__(self, proc_dir):
        self.current_processing_file = ''
        self.current_processing_file_total_records = 0
        self.sample_size = 0
        self.sample_records = []
        self.proc_dir = proc_dir

    def get_sample_size(self):
        # Get total record count
        with open(os.path.join(self.proc_dir, self.current_processing_file), 'r') as f:
            self.current_processing_file_total_records = (len(f.readlines())) - 1

        # Get total sample size, either 25 or 1% of total, whichever is more
        self.sample_size = (min(int(max(25.0, self.current_processing_file_total_records * .01)),
                                self.current_processing_file_total_records))

    def get_sample_records(self):
        # Returns a list of records to be used as samples
        self.sample_records = set(random.sample(range(1, self.current_processing_file_total_records),
                                                (self.sample_size - 1)))

    def set_processing_file(self, file):
        self.current_processing_file = file
        self.get_sample_size()
        self.get_sample_records()


class PregProcess:
    def __init__(self, param):
        self.week_number = param['week_number']
        self.data_source_dir = ''
        self.merge_source_dir = ''
        self.data_source_file = ''
        self.merge_source_files = []
        self.db_headers = ['Acct_Key', 'Grp_Num', 'Billing_Unit',
                           'Custom_Gift_Card_Amt', 'Logical_Person_Key',
                           'First', 'Last', 'WellmarkID', 'Add1', 'Add2',
                           'City', 'St', 'Zip', 'Fulfillment_Question_Id',
                           'FormNum', 'Program']

        self.compare_fields = ['WellmarkID', 'First', 'Last', 'Add1', 'Add2',
                               'City', 'St', 'Zip', 'FormNum']

    def get_source_dirs(self):
        date_dir = re.compile("([0-9])*-([0-9])*-([0-9])*")
        proc_dir = [f for f in os.listdir(os.path.curdir) if date_dir.search(f)]

        dated_folders = {}
        for folder in proc_dir:
            dated_folders[folder] = datetime.datetime.strptime(folder, "%m-%d-%y")

        sorted_folders = sorted(dated_folders.items(), key=lambda kv: kv[1])

        self.merge_source_dir = os.path.join(os.curdir, 'Week {0}'.format(self.week_number))
        self.data_source_dir = os.path.join(os.curdir, sorted_folders[(self.week_number - 1)][0])

    def get_source_files(self):
        try:
            self.data_source_file = ([f for f in os.listdir(self.data_source_dir)
                                      if str(f[-3:]).upper() == 'TXT'])[0]
            self.merge_source_files = ([f for f in os.listdir(self.merge_source_dir)
                                        if str(f[-3:]).upper() == 'TXT'])
        except IndexError:
            print('Error!! Source file not found')

    def import_data(self):
        with open(os.path.join(self.data_source_dir, self.data_source_file), 'r') as s:
            csvr = csv.DictReader(s, delimiter='\t')
            fieldnames = self.db_headers
            # fieldnames = list(map(lambda x: x.replace('#', ''), fieldnames))
            # print(fieldnames)
            db_fields = ' VARCHAR(100), '.join(fieldnames)

            sqldb = sqlite3.connect('source_data.db')
            c = sqldb.cursor()
            c.execute("DROP TABLE IF EXISTS source_data;")
            c.execute("VACUUM;")
            c.execute("CREATE TABLE source_data ({0});".format(db_fields))

            placeholders = ', '.join(['?'] * len(fieldnames))
            fields = ', '.join(fieldnames)

            for line in csvr:
                sql = "INSERT INTO source_data ({0}) VALUES ({1});".format(fields, placeholders)
                c.execute(sql, list(line.values()))

            c.execute("UPDATE source_data SET Add2 = REPLACE(Add2, '_', '') ;")
            c.execute("UPDATE source_data SET FormNum = (FormNum||'.pdf');")
            sqldb.commit()
            sqldb.close()


class CSPProcess:
    def __init__(self, param):
        self.week_number = param['week_number']
        # Path to Pregnancy, location of the source files
        self.preg_path = os.path.abspath(
                os.path.join("//JTSRV4", "Data", "Customer Files",
                             "In Progress", "WM Pregnancy In-Sourcing")
        )
        #
        self.data_source_dir = ''
        self.merge_source_dir = ''
        self.data_source_file = ''
        self.merge_source_files = []
        self.db_headers = ['Acct_Key', 'Grp_Num', 'Billing_Unit',
                           'Custom_Gift_Card_Amt', 'Logical_Person_Key',
                           'First', 'Last', 'WellmarkID', 'Add1', 'Add2',
                           'City', 'St', 'Zip', 'Fulfillment_Question_Id',
                           'FormNum', 'Program']

        self.compare_fields = ['WellmarkID', 'First', 'Last', 'Add1', 'Add2',
                               'City', 'St', 'Zip']

    def get_source_dirs(self):
        date_dir = re.compile("([0-9])*-([0-9])*-([0-9])*")
        month_search = re.compile("[A-Z, a-z]{3}(?=[\s][0-9]*[\s](CSP))")
        csp_month = month_search.search(os.path.basename(os.getcwd())).group(0)
        source_search = re.compile("({0})(?=[\s][0-9]*[\s](Preg In-Sourcing))".format(csp_month))

        # Source directory for processing month pregnancy (source data)
        job_dir = [f for f in os.listdir(self.preg_path) if source_search.search(f)]
        proc_dir = [f for f in os.listdir(os.path.join(self.preg_path, job_dir[0]))
                    if date_dir.search(f)]
        #

        dated_folders = {}
        for folder in proc_dir:
            dated_folders[folder] = datetime.datetime.strptime(folder, "%m-%d-%y")

        sorted_folders = sorted(dated_folders.items(), key=lambda kv: kv[1])

        self.merge_source_dir = os.path.join(os.curdir, 'Week {0}'.format(self.week_number))
        self.data_source_dir = os.path.join(self.preg_path, job_dir[0], sorted_folders[(self.week_number - 1)][0])

    def get_source_files(self):
        try:
            self.data_source_file = ([f for f in os.listdir(self.data_source_dir)
                                      if str(f[-3:]).upper() == 'TXT'])[0]
            self.merge_source_files = ([f for f in os.listdir(self.merge_source_dir)
                                        if str(f[-3:]).upper() == 'TXT'])
        except IndexError:
            print('Error!! Source file not found')

    def import_data(self):
        with open(os.path.join(self.data_source_dir, self.data_source_file), 'r') as s:
            csvr = csv.DictReader(s, delimiter='\t')
            fieldnames = self.db_headers
            # fieldnames = list(map(lambda x: x.replace('#', ''), fieldnames))
            # print(fieldnames)
            db_fields = ' VARCHAR(100), '.join(fieldnames)

            sqldb = sqlite3.connect('source_data.db')
            c = sqldb.cursor()
            c.execute("DROP TABLE IF EXISTS source_data;")
            c.execute("VACUUM;")
            c.execute("CREATE TABLE source_data ({0});".format(db_fields))

            placeholders = ', '.join(['?'] * len(fieldnames))
            fields = ', '.join(fieldnames)

            for line in csvr:
                sql = "INSERT INTO source_data ({0}) VALUES ({1});".format(fields, placeholders)
                c.execute(sql, list(line.values()))

            c.execute("UPDATE source_data SET Add2 = REPLACE(Add2, '_', '') ;")
            c.execute("UPDATE source_data SET FormNum = (FormNum||'.pdf');")
            sqldb.commit()
            sqldb.close()


class CSPMerge:
    def __init__(self, proc_dir):
        self.current_processing_file = ''
        self.current_processing_file_total_records = 0
        self.sample_size = 0
        self.sample_records = []
        self.proc_dir = proc_dir

    def get_sample_size(self):
        # Get total record count
        with open(os.path.join(self.proc_dir, self.current_processing_file), 'r') as f:
            self.current_processing_file_total_records = (len(f.readlines())) - 1

        # Get total sample size, either 25 or 1% of total, whichever is more
        self.sample_size = (min(int(max(25.0, self.current_processing_file_total_records * .01)),
                                self.current_processing_file_total_records))

    def get_sample_records(self):
        # Returns a list of records to be used as samples
        self.sample_records = set(random.sample(range(1, self.current_processing_file_total_records),
                                                (self.sample_size - 1)))

    def set_processing_file(self, file):
        self.current_processing_file = file
        self.get_sample_size()
        self.get_sample_records()


def display_diff(x, y):
    s = ("", 0)
    if str.strip(x).upper() != str.strip(y).upper():
        s = ("*** INVALID MATCH ***", 1)
    return s


def run_membership(param):
    proc_dir = [f for f in os.listdir(os.path.curdir) if f[0:5] == str(param['job_number'])]
    if len(proc_dir) != 1:
        print("Error, Invalid search")
        time.sleep(3)
        sys.exit()

    source_data = MembershipSource(param)
    merge_data = MembershipMerge(param)

    merge_data.get_processing_dir()

    source_data.get_processing_dir()
    source_data.get_filename(merge_data.process_state)
    source_data.import_data()

    merge_data.get_split_1_files()
    merge_data.get_split_2_files()

    flds = MembershipFields()
    compare_fields = flds.get_fieldnames(merge_data.process_state)

    # print("Merge\n", merge_data.__dict__)
    # print("Source\n", source_data.__dict__)
    # return

    sqldb = sqlite3.connect('source_data.db')
    sqldb.row_factory = sqlite3.Row
    c = sqldb.cursor()

    for file in (merge_data.split_1_files if param['split'] == 0 else merge_data.split_2_files):
        merge_data.set_processing_file(file)

        if not os.path.isdir(os.path.join(source_data.proc_dir, 'data check')):
            os.mkdir(os.path.join(source_data.proc_dir, 'data check'))

        with open(os.path.join(source_data.proc_dir, 'data check',
                               "{0}_DATA CHECK REPORT.txt".format(merge_data.current_processing_file[:-4])), 'w+') as s:

            # s.write("Comparing: {0} --> {1}".format(merge_data.current_processing_file, source_data.filename))
            s.write("Comparing: {2}\\{0} -->\n{4:>11}{3}\\{1}\n".format(merge_data.current_processing_file,
                                                                        source_data.filename,
                                                                        os.path.abspath(merge_data.proc_dir),
                                                                        os.path.abspath(source_data.proc_dir),
                                                                        " "))

            errors = 0

            with open(os.path.join(merge_data.proc_dir, merge_data.current_processing_file), 'r') as f:
                csvr = csv.DictReader(f, delimiter='\t')
                for n, line in enumerate(csvr, 1):
                    if n in merge_data.sample_records:
                        c.execute("SELECT * FROM source_data WHERE [account]=?;", (line['account'],))
                        match_result = c.fetchone()
                        s.write("\n")
                        for field in compare_fields:
                            match_eval = display_diff(line[field], match_result[field])
                            s.write("\t{3}: {0} --> {1} {2}\n".format(line[field],
                                                                      match_result[field],
                                                                      match_eval[0],
                                                                      field))
                            if match_eval[1]:
                                print('\t{1}: Account {0} ERROR'.format(
                                        line['account'], merge_data.current_processing_file))
                                errors += 1
                s.write("\n")

            s.write("Processing Finished: {0} total errors".format(errors))

    sqldb.close()


def run_hcm(param):
    hcm = HCMProcess(param)
    hcm.get_source_dirs()
    hcm.get_source_files()
    hcm.get_sample_size()
    hcm.get_sample_records()
    hcm.import_data()

    if not os.path.isdir(os.path.join(hcm.merge_source_dir, 'data check')):
        os.mkdir(os.path.join(hcm.merge_source_dir, 'data check'))

    sqldb = sqlite3.connect('source_data.db')
    sqldb.row_factory = sqlite3.Row
    c = sqldb.cursor()

    with open(os.path.join(hcm.merge_source_dir, 'data check',
                           "{0}_DATA CHECK REPORT.txt".format(hcm.merge_source_file[:-4])), 'w+') as s:

        # Compare merge file --> source file
        s.write("Comparing: {2}\\{0} -->\n{4:>11}{3}\\{1}\n".format(hcm.merge_source_file,
                                                                    hcm.data_source_file,
                                                                    os.path.abspath(hcm.merge_source_dir),
                                                                    os.path.abspath(hcm.data_source_dir),
                                                                    " "))
        errors = 0

        with open(os.path.join(hcm.merge_source_dir, hcm.merge_source_file), 'r') as f:
            csvr = csv.DictReader(f, delimiter='\t')
            for n, line in enumerate(csvr, 1):
                if n in hcm.sample_records:
                    c.execute("SELECT * FROM source_data WHERE Wellmark_ID=?;", (line['Wellmark_ID'],))
                    match_result = c.fetchone()
                    s.write("\n")
                    for field in hcm.compare_fields:
                        match_eval = display_diff(line[field], match_result[field])
                        s.write("\t{3}: {0} --> {1} {2}\n".format(line[field],
                                                                  match_result[field],
                                                                  match_eval[0],
                                                                  field))
                        if match_eval[1]:
                            print('\t{1}: WellmarkID {0} ERROR'.format(
                                    line['Wellmark_ID'], hcm.merge_source_file))
                            errors += 1
            s.write("\n")

        s.write("Processing Finished: {0} total errors".format(errors))

        sqldb.close()


def run_pregnancy(param):
    preg = PregProcess(param)

    preg.get_source_dirs()
    preg.get_source_files()
    preg_merge = PregMerge(preg.merge_source_dir)
    preg.import_data()

    if not os.path.isdir(os.path.join(preg.merge_source_dir, 'data check')):
        os.mkdir(os.path.join(preg.merge_source_dir, 'data check'))

    sqldb = sqlite3.connect('source_data.db')
    sqldb.row_factory = sqlite3.Row
    c = sqldb.cursor()

    for file in preg.merge_source_files:
        preg_merge.set_processing_file(file)

        if not os.path.isdir(os.path.join(preg.merge_source_dir, 'data check')):
            os.mkdir(os.path.join(preg.merge_source_dir, 'data check'))

        with open(os.path.join(preg.merge_source_dir, 'data check',
                               "{0}_DATA CHECK REPORT.txt".format(preg_merge.current_processing_file[:-4])), 'w+') as s:

            s.write("Comparing: {2}\\{0} -->\n{4:>11}{3}\\{1}\n".format(preg_merge.current_processing_file,
                                                                        preg.data_source_file,
                                                                        os.path.abspath(preg_merge.proc_dir),
                                                                        os.path.abspath(preg.merge_source_dir),
                                                                        " "))
            errors = 0

            with open(os.path.join(preg.merge_source_dir, preg_merge.current_processing_file), 'r') as f:
                csvr = csv.DictReader(f, delimiter='\t')
                for n, line in enumerate(csvr, 1):
                    if n in preg_merge.sample_records:
                        c.execute('SELECT * FROM source_data '
                                  'WHERE WellmarkID=? AND FormNum=?;', (line['WellmarkID'], line['FormNum']))
                        match_result = c.fetchone()
                        if match_result is not None:
                            s.write("\n")
                            for field in preg.compare_fields:
                                match_eval = display_diff(line[field], match_result[field])
                                s.write("\t{3}: {0} --> {1} {2}\n".format(line[field],
                                                                          match_result[field],
                                                                          match_eval[0],
                                                                          field))
                                if match_eval[1]:
                                    print('\t{1}: WellmarkID {0} ERROR'.format(
                                            line['WellmarkID'], preg_merge.current_processing_file))
                                    errors += 1
                s.write("\n")

            s.write("Processing Finished: {0} total errors".format(errors))

    sqldb.close()


def run_csp(param):
    csp = CSPProcess(param)
    csp.get_source_dirs()

    csp.get_source_files()
    csp_merge = CSPMerge(csp.merge_source_dir)
    csp.import_data()

    if not os.path.isdir(os.path.join(csp.merge_source_dir, 'data check')):
        os.mkdir(os.path.join(csp.merge_source_dir, 'data check'))

    sqldb = sqlite3.connect('source_data.db')
    sqldb.row_factory = sqlite3.Row
    c = sqldb.cursor()

    for file in csp.merge_source_files:
        csp_merge.set_processing_file(file)

        if not os.path.isdir(os.path.join(csp.merge_source_dir, 'data check')):
            os.mkdir(os.path.join(csp.merge_source_dir, 'data check'))

        with open(os.path.join(csp.merge_source_dir, 'data check',
                               "{0}_DATA CHECK REPORT.txt".format(csp_merge.current_processing_file[:-4])), 'w+') as s:

            s.write("Comparing: {2}\\{0} -->\n{4:>11}{3}\\{1}\n".format(csp_merge.current_processing_file,
                                                                        csp.data_source_file,
                                                                        os.path.abspath(csp_merge.proc_dir),
                                                                        os.path.abspath(csp.merge_source_dir),
                                                                        " "))
            errors = 0

            with open(os.path.join(csp.merge_source_dir, csp_merge.current_processing_file), 'r') as f:
                csvr = csv.DictReader(f, delimiter='\t')
                for n, line in enumerate(csvr, 1):
                    if n in csp_merge.sample_records:
                        c.execute('SELECT * FROM source_data '
                                  'WHERE WellmarkID=?;', (line['WellmarkID'],))
                        match_result = c.fetchone()
                        # if match_result is not None:
                        s.write("\n")
                        for field in csp.compare_fields:
                            match_eval = display_diff(line[field], match_result[field])
                            s.write("\t{3}: {0} --> {1} {2}\n".format(line[field],
                                                                      match_result[field],
                                                                      match_eval[0],
                                                                      field))
                            if match_eval[1]:
                                print('\t{1}: WellmarkID {0} ERROR'.format(
                                        line['WellmarkID'], csp_merge.current_processing_file))
                                errors += 1
                s.write("\n")

            s.write("Processing Finished: {0} total errors".format(errors))

    sqldb.close()


def instructions():
    print("This will data check jobs for FB Membership, WM HCM, WM CSP, and WM Pregnancy\n\n"
          "For FB Membership, place this file:\n\t"
          "FB membership Cards\\data\\[*here*]\\##### [ST] Membership Cards...\n\n"
          "For WM HCM, place this file:\n\t"
          "WM HCM ONLY Cards\\##### WM [Mon] [YYYY] HCM Only\\[*here*]\\\n\n"
          "For WM Pregnancy, place this file:\n\t"
          "WM Pregnancy In-Sourcing\\##### WM [Mon] [YYYY] Preg In-Sourcing\\[*here*]\\\n\n"
          "For WM CSP, place this file:\n\t"
          "WM CSP\\##### WM [Mon] [YYYY] CSP\\[*here*]\\\n\n"
          "Execute, follow on screen instructions\nAudit reports are saved in "
          "the same directory as the merge files in .\\data\\\n\n")


def questions():

    parameters = {}
    # parameters = {'process': 3, 'week_number': 1}

    try:
        qry = int(input("Processing job FB Membership (0), WM HCM (1), WM Pregnancy (2), CSP (3): "))
        if qry not in [0, 1, 2, 3]:
            raise ValueError
        else:
            parameters['process'] = qry

        # FB Membership processing
        if qry == 0:
            fb_qry_1 = int(input("IA: First, Paid Welcome (0)\n"
                                 "SD: First, 2nd, Paid (0)\n"
                                 "UT: First, 2nd (0)\n"
                                 "NM: First, Paid (0)\n"
                                 "IA: 2nd (1)\n"
                                 "UT: Paid, Welcome (1)\n"
                                 "SD, NM: Welcome (1): "))
            if fb_qry_1 not in [0, 1]:
                raise ValueError
            else:
                parameters['split'] = fb_qry_1

            fb_qry_2 = int(input("Job number: "))
            parameters['job_number'] = fb_qry_2

        # WM HCM processing
        if qry == 1:
            hcm_qry = int(input("Week # (numeric)? "))
            parameters['week_number'] = hcm_qry

        # WM Pregnancy processing
        if qry == 2:
            preg_qry = int(input("Week # (numeric)? "))
            parameters['week_number'] = preg_qry

        # CSP processing
        if qry == 3:
            csp_qry = int(input("Week # (numeric)? "))
            parameters['week_number'] = csp_qry

    except ValueError:
        print("Invalid response, cancelling")
        time.sleep(4)
        sys.exit()

    if parameters['process'] == 0:
        run_membership(parameters)
    if parameters['process'] == 1:
        run_hcm(parameters)
    if parameters['process'] == 2:
        run_pregnancy(parameters)
    if parameters['process'] == 3:
        run_csp(parameters)

    print("Processing Completed for {0}".format(['FB Membership',
                                                 'WM HCM',
                                                 'WM Pregnancy',
                                                 'CSP'][parameters['process']]))
    time.sleep(2.5)
    # os.remove('source_data.db')
    # sys.exit()


if __name__ == '__main__':
    ans = int(input("Instructions (0), Begin (1): "))
    if not ans:
        instructions()
    questions()
