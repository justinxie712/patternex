#!usr/bin/python
import subprocess

# Global variables
list_prev_count = []
list_cur_count = []
list_percentage = []
list_num = []
node_in = ['172.31.4.130']
#node_in = ['172.31.4.130', '172.31.3.103', '172.31.3.200']
node_list = []
table_list = []      # string that contains all tables
table_defs = []      # list of table definitions
where_clause = ""    # WHERE Clause string
testNum = 1          # Test Counter
cur_yaml = ""        # Entire string of YAML file
insert_sh = ""       # insert sh file
range1_sh = ""       # range1 sh file
mixed_sh = ""        #  mixed sh file
simple1 = ""
range1 = ""
varchar = {}
timestamp = {}
double = {}
uuid = {}
text = {}
timeuuid = {}
results_str = ""
results = []
cql_file = ""        # the read lines of cql file
cur_table = ""       # current table at focus
data_type = ""       # extracted string from cur_table that identifies data type
indiv_spec = ""      # one full columnspec string that includes size and data name
column_spec = []     # complete columnspec
list_data_names = [] # list of all the names of the datatypes
table_name = ""      # label for table name
spec_size = ""       # size distribution for columnspec
spec_name = ""       # name of datatype for columnspec
yaml_template = ""

# Table class definition
class tableClass():
    global yaml_template

    # reset yaml file template
    @staticmethod
    def reset_yaml_temp():
        global yaml_template
        yaml_template = """#
# Keyspace info
#
keyspace: stresscqlOne

keyspace_definition: |
  CREATE KEYSPACE stresscqlOne WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

#
# Table info
#
table: %s

table_definition: |
%s

columnspec:
%s

insert:
  partitions: uniform(1..50)
  batchtype: LOGGED
  select: uniform(1..10)/10

#
# A list of queries you wish to run against the schema
#
queries:
   simple1:
      cql: %s
      fields: samerow
   range1:
      cql: %s
      fields: samerow

token_range_queries:
  all_columns_tr_query:
    columns: '*'
    page_size: 5000
"""
    # Retrieve all tables from cql file
    @staticmethod
    def get_all_tables():
        global cql_file
        lines = open('ptrx.cql', "r+")
        cql_file = lines.readlines()

        global table_list
        global table_defs
        table = ""
        for line in cql_file:
            if line.startswith("CREATE TABLE"):
                table += " " + line
            elif line.startswith("PRIMARY KEY ("):
                table += " " + line
            elif line.startswith(");"):
                table += " );" + "\n"
                table_list.append(table)
                ll = "".join(table_list)
                table_defs.append(ll)
                table = ""
                table_list = []
            elif "," in line:
                table += " " + line
        "".join(table_defs)

    # Return the table from the yaml file
    @staticmethod
    def get_table(index):
        table = table_defs[index]
        return table

    # Return individual column's specs
    @staticmethod
    def create_spec(name, size):
        indiv_spec = """    - name: %s
          %s""" % (name, size) + "\n"

        return indiv_spec

    # Return the datatype from the yaml file and puts all data names into a list (CONTINUE IT)
    @staticmethod
    def get_spec_data(index):
        global list_data_names

        x = table_defs[index]
        y = x.split()
        count = 0
        for word in y:
            global spec_name
            global spec_size

            if "map" in word:
                pass

            if word == "varchar,":
                spec_name = y[count - 1]
                spec_size = """size: uniform(5..40)
                    population: uniform(1..10M)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            if word == "timestamp":
                spec_name = y[count - 1]
                spec_size = """size: fixed(1000)
                    population: fixed(1000)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            if word == "double":
                spec_name = y[count - 1]
                spec_size = """size: fixed(16)
                    population: uniform(1..4)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            if word == "uuid":
                spec_name = y[count - 1]
                spec_size = """size: fixed(16)
                    population: uniform(0..100B)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            if word == "text":
                spec_name = y[count - 1]
                spec_size = """size: uniform(5..40)
                    population: uniform(1..4)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            #if word == "timeuuid":
                #spec_name = y[count - 1]
                #spec_size = """size: uniform(2..5)
                #    population: uniform(1..4)"""
                #p = tableClass.create_spec(spec_name, spec_size)
                #column_spec.append(p)
                #list_data_names.append(spec_name)
                #p = ""
                #spec_name = ""
                #spec_size = ""

            if word == "int":
                spec_name = y[count - 1]
                spec_size = """size: fixed(4)
                    population: uniform(1..4B)"""
                p = tableClass.create_spec(spec_name, spec_size)
                column_spec.append(p)
                list_data_names.append(spec_name)
                p = ""
                spec_name = ""
                spec_size = ""

            count += 1
        data = "".join(column_spec)
        return data

    # Return WHERE CLAUSE
    @staticmethod
    def get_partition_key(index):
        global table_defs
        global where_clause
        j = table_defs[index].split()
        count = 0
        for word in j:
            if word == "KEY":
                y = j[count + 1]  # first partition key column
                p = j[count + 2]  # second partition key column
                if y.count("(") > 1:
                    where_clause = "WHERE " + y[2:-1] + " AND " + p[:-2] + " = ?"
                else:
                    where_clause = "WHERE " + y[1:-1] + " = ?"
            count += 1

    # Set queries
    @staticmethod
    def get_queries(index,tablename,where,querydata):
        global table_defs
        global simple1
        global range1
        x = table_defs[index]
        y = x.split()
        for terms in y:
            if "hpafeatures_bymin" in terms:
                simple1 = "select * from %s %s LIMIT 100" % (tablename, where)
                range1 = """SELECT ipv4, day_ts, login_success, login_failure, num_chkout, num_passwd, num_requests FROM hpafeatures_bymin WHERE minute_ts= ? AND ipv4=?"""
            elif "ipusermap_byday" in terms:
                simple1 = "select * from %s %s LIMIT 100" % (tablename, where)
                range1 = """SELECT userids FROM ipusermap_byday WHERE day_ts= ? AND ipv4= ?"""
            else:
                simple1 = "select * from %s%s LIMIT 100" % (tablename, where)
                range1 = "select %s from %s LIMIT 100" % (querydata, tablename)


    # Return table name
    @staticmethod
    def get_table_name(index):
        table = table_defs[index]
        terms = table.split()
        name = terms[2]
        name = name[:-1]
        return name

    # Return the complete yaml file
    @staticmethod
    def create_yaml(yamlfile, index):
        global yaml_template
        global list_data_names
        global where_clause
        global range1
        global simple1

        # Return all yaml file specs
        tablename = tableClass.get_table_name(index)
        tabledef = tableClass.get_table(index)
        columnspec = tableClass.get_spec_data(index)
        querydata = ", ".join(list_data_names)
        tableClass.get_partition_key(index)
        tableClass.get_queries(index,table_name,where_clause,querydata)
        yaml_template = yaml_template % (tablename, tabledef, columnspec, simple1, range1)

        with open(yamlfile, "w") as file_descriptor:
            file_descriptor.write(yaml_template)
            file_descriptor.close()

    # Reset all reuseable variables
    @staticmethod
    def reset_variables():
        global cur_table
        global datatype
        global indiv_spec
        global column_spec
        global list_data_names
        global table_name
        global spec_size
        global spec_name
        global insert_sh
        global range1_sh
        global mixed_sh
        global where_clause
        global range1
        global simple1

        tableClass.reset_yaml_temp()
        cur_table = ""
        datatype = ""
        indiv_spec = ""
        column_spec = []
        list_data_names = []
        table_name = ""
        spec_size = ""
        spec_name = ""
        insert_sh = ""
        range1_sh = ""
        mixed_sh = ""
        where_clause = ""
        range1 = ""
        simple1 = ""

# Testing class definition
class testClass():
    # Retrieves the strings of the of the results
    @staticmethod
    def get_results(list_info, list_final):
        for x in list_info:
            list_chars = x.split()
            list3 = ([int(s) for s in list_chars if s.isdigit()])
            for i in list3:
                num = list3.pop()
                list_final.append(num)

    # Retrieves the percentages in strings of the results list
    @staticmethod
    def get_percentage(list_info, list_final):
        for x in list_info:
            list_chars = x.split()
            list_final.append(list_chars[len(list_chars) - 1])

    # Retrieves the int of each percentage
    @staticmethod
    def get_num(list_info, list_final):
        for x in list_info:
            cell = int(x.strip("%\n "))
            list_final.append(cell)

    # Create yaml files
    @staticmethod
    def create_yaml_label(index):
        global table_defs
        tablename = tableClass.get_table_name(index)
        yaml_file = "cql_%s.yaml" % tablename
        return yaml_file

    # Create sh files
    @staticmethod
    def create_insert_sh(listofnodes, index, yaml_file):
        global insert_sh
        name = tableClass.get_table_name(index)
        sh_file = 'stress_with_insert_and_%s.sh' % name
        insert_sh = sh_file
        log = 'log_insert_with_%s.txt' % yaml_file
        text_file = open(sh_file, "w")
        text_file.write("NODES=%s" % (listofnodes) + "\n")
        text_file.write("/usr/bin/cassandra-stress user profile=/home/centos/Downloads/%s ops\(insert=1\) n=100000 -node $NODES &> %s" % (yaml_file, log) + "\n")
        subprocess.check_call('chmod a+x %s' % sh_file, shell=True)
        return log
        text_file.close()

    @staticmethod
    def create_range1_sh(listofnodes,index,yaml_file):
        global range1_sh
        name = tableClass.get_table_name(index)
        sh_file = 'stress_with_range1_and_%s.sh' % name
        range1_sh = sh_file
        log = 'log_range1_with_%s.txt' % yaml_file
        text_file = open(sh_file, "w")
        text_file.write("NODES=%s" % (listofnodes) + "\n")
        text_file.write("/usr/bin/cassandra-stress user profile=/home/centos/Downloads/%s ops\(range1=1\) n=100000 -node $NODES &> %s" % (yaml_file, log) + "\n")
        subprocess.check_call('chmod a+x %s' % sh_file, shell=True)
        return log
        text_file.close()

    @staticmethod
    def create_mixed_sh(listofnodes,index,yaml_file):
        global mixed_sh
        name = tableClass.get_table_name(index)
        sh_file = 'stress_with_mixed_and_%s.sh' % name
        mixed_sh = sh_file
        log = 'log_mixed_with_%s.txt' % yaml_file
        text_file = open(sh_file, "w")
        text_file.write("NODES=%s" % (listofnodes) + "\n")
        text_file.write("/usr/bin/cassandra-stress user profile=/home/centos/Downloads/%s ops\(range1=1,insert=1\) n=100000 -node $NODES &> %s" % (yaml_file, log) + "\n")
        subprocess.check_call('chmod a+x %s' % sh_file, shell=True)
        return log
        text_file.close()

    # Retrieve results and pipe to text files
    @staticmethod
    def result(filetext, filewrite, test_type, num):
        global results_str
        global results
        imp_info = []
        tc_info = []

        threadcount_info = 'Running with'
        improvement_info = 'Improvement over'

        # Read the file
        with open(filetext, "r+") as text_insert:
            for line in text_insert:
                if improvement_info in line:
                    imp_info.append(line)
                if threadcount_info in line:
                    tc_info.append(line)
                if "Results:" in line:
                    for info in range(17):
                        k = text_insert.next()
                        results_str += k
                    results.append(results_str)
                    "".join(results)
                    results_str = ""
            tc_info.remove(tc_info[0])
        text_insert.close()

        # Write to the file of the information
        text_file = open(filewrite, "a")
        prompt = "Results of Test %s: Testing table %s with %s" % (str(testNum), str(tableClass.get_table_name(num)), str(test_type) + "\n")
        text_file.write(prompt)
        text_file.write("  \n")

        # Filter and search for improvement and threadcount statements
        testClass.get_results(imp_info, list_prev_count)
        testClass.get_results(tc_info, list_cur_count)
        testClass.get_percentage(imp_info, list_percentage)
        testClass.get_num(list_percentage, list_num)

        index = list_num.index(max(list_num))
        message = """The best results comes with a threadcount of %s threads, which has a %s%% increase in performance over the previous test of %s threads.
        The results are:
        %s""" % (str(list_cur_count[index]), str(list_num[index]), str(list_prev_count[index]), str(results[index]))

        text_file.write(message)
        text_file.write("  \n")
        text_file.write("==================================================================================================================================")
        text_file.write("  \n")

        list_prev_count[:] = []
        list_cur_count[:] = []
        list_percentage[:] = []
        list_num[:] = []

        text_file.close()


# Main function
# Iterates through the list of IP addresses of the nodes and runs each test on them in increasingly bigger lists
def main():
    global testNum
    global table_defs
    global cur_yaml
    global insert_sh
    global range1_sh
    global mixed_sh

    tableClass.reset_yaml_temp()
    nodeIns = testClass()
    yamlObj = tableClass()

    yamlObj.get_all_tables()

    table_num = 0
    for table in table_defs:
        # Create yaml file
        test_type = ""
        yaml_name = nodeIns.create_yaml_label(table_num)
        cur_yaml = yamlObj.create_yaml(yaml_name,table_num)

        for node in node_in:
            node_list.append(node)
            nodes = ",".join(node_list)

            ########################################################################################################
            # Write to stress_with_insert.sh
            log_insert = nodeIns.create_insert_sh(nodes,table_num,yaml_name)

            # To run the first stress test with insert
            test_type = "Insert"
            print "TEST %d: Stress with Insert In Progress" % testNum
            subprocess.check_call('/home/centos/Downloads/%s' % insert_sh, shell=True)
            nodeIns.result(log_insert, 'resultOne.txt', test_type, table_num)
            print "TEST %d and REPORT COMPLETE" % (testNum)
            print " "
            testNum += 1
            test_type = ""
            subprocess.check_call('rm %s' % insert_sh, shell=True)

            ########################################################################################################
            # Write to stress_with_range1.sh
            log_range1 = nodeIns.create_range1_sh(nodes,table_num,yaml_name)

            # To run the second stress test with range1
            test_type = "Range1"
            print "TEST %d: Stress with Range1 In Progress" % testNum
            subprocess.check_call('/home/centos/Downloads/%s' % range1_sh, shell=True)
            nodeIns.result(log_range1, 'resultTwo.txt', test_type, table_num)
            print "TEST %d and REPORT COMPLETE" % (testNum)
            print " "
            testNum += 1
            test_type = ""
            subprocess.check_call('rm %s' % range1_sh, shell=True)

            ########################################################################################################
            # Write to stress_with_mixed.sh
            log_mixed = nodeIns.create_mixed_sh(nodes,table_num,yaml_name)

            # To run the third stress test with mixed
            test_type = "Mixed"
            print "TEST %d: Stress with Mixed In Progress" % testNum
            subprocess.check_call('/home/centos/Downloads/%s' % mixed_sh, shell=True)
            nodeIns.result(log_mixed, 'resultThree.txt', test_type, table_num)
            print "TEST %d and REPORT COMPLETE" % (testNum)
            print " "
            testNum += 1
            test_type = ""
            subprocess.check_call('rm %s' % mixed_sh, shell=True)

        cur_yaml = ""
        table_num += 1
        yamlObj.reset_variables()

    print "All tests ran successfully with no errors."


main()