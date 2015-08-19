#!/usr/bin/python
import sys
import re, getopt
import MySQLdb as mdb
from itertools import islice
import os
from os.path import basename

def parseArg(argv):
    global database_dbhost
    global database_dbname
    global database_username
    global database_password
    global _mode
    global _InputFile
    global logfilename

    database_dbhost = 'localhost'
    database_dbname = 'mira_final'
    database_username = 'huong'
    database_password = '123456'

    try:
        opts, args = getopt.getopt(argv,"hi:m:",["ifile=","mode="])
    except getopt.GetoptError:
        print 'test.py -i <inputfile> -m <mode>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'test.py -i <inputfile> -m <mode>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            _InputFile = arg
        elif opt in ("-m", "--mode"): 
            _mode = arg
 
    basename = os.path.basename(_InputFile)
    n = re.search('(.+).darshan.gz.input.txt',basename)
    if n:
        tmp = n.group(1)
        logfilename = tmp[:100]
    else:
        logfilename = basename

    sys.stdout.flush()

def create_table():

    conn = mdb.connect(database_dbhost, database_username, database_password, database_dbname)
    with conn:
        cur = conn.cursor()
        s=("CREATE TABLE if not exists jobs_info(logid INT(11) NOT NULL AUTO_INCREMENT,logfilename VARCHAR(100) , projid VARCHAR(20), uid VARCHAR(20),  jobid VARCHAR(20) ,exe VARCHAR(100), nprocs INT, runtime INT, start_time DATETIME, end_time DATETIME, appname VARCHAR(50),"
             "total_bytes bigint, local_iotime float, local_meta float,"
             "global_iotime float, global_meta float, shared_time_by_open float, shared_time_by_open_lastio float, shared_time_by_slowest float,"
             "agg_perf_by_cumul float, agg_perf_by_open float, agg_perf_by_open_lastio float, agg_perf_by_slowest float,"
             "iotime_by_cumul float, iotime_by_open float, iotime_by_open_lastio float, iotime_by_slowest float, iotime float,agg_perf_MB float, "
             "total_count int, total_size bigint, total_max_offset bigint,"
             "read_only_count int, read_only_size bigint, read_only_max_offset bigint,"
             "write_only_count int, write_only_size bigint, write_only_max_offset bigint,"
             "read_write_count int, read_write_size bigint, read_write_max_offset bigint,"
             "unique_count int, unique_size bigint, unique_max_offset bigint,"
             "shared_count int, shared_size bigint, shared_max_offset bigint,"
             "partshared_count int, allshared_count int, allshared_posix_count int,allshared_mpi_count int, partshared_posix_count int,partshared_mpi_count int, "
             "unique_posix_count int, unique_mpi_count int,"
             "allshared_posix_readwrite int, allshared_posix_read int, allshared_posix_write int,allshared_mpi_readwrite int, allshared_mpi_read int, allshared_mpi_write int,partshared_posix_readwrite int, partshared_posix_read int, partshared_posix_write int,partshared_mpi_readwrite int, partshared_mpi_read int, partshared_mpi_write int,unique_posix_readwrite int, unique_posix_read int, unique_posix_write int, unique_mpi_readwrite int, unique_mpi_read int, unique_mpi_write int,"
             "PRIMARY KEY(logid))")           	
        #print(s)
        cur.execute(s)
        
        
#def removeDB():
#    conn = mdb.connect(database_dbhost, database_username, database_password,database_dbname)
#    with conn:
#        cur = conn.cursor()
#        cur.execute("DROP TABLE IF EXISTS jobs_info;")
#        print 'Delete old database - should remove after testing period'
#        sys.stdout.flush()

def parse_header(cur,next_line_group,logfilename):
#        jobs_info(logid INT(11) NOT NULL AUTO_INCREMENT,logfilename VARCHAR(100) , projid VARCHAR(20), uid VARCHAR(20),  jobid VARCHAR(20) ,exe VARCHAR(100), nprocs INT, runtime INT, start_time DATETIME, end_time DATETIME, appname VARCHAR(50),"
    proj = 0
    for line in next_line_group:
        #print "%s " % line
        n = re.search('#\s*exe:\s*(\S+)\s+(.*)',line)
        if n:
            tmp = n.group(1)
            exe = tmp[-100:]
            appname = basename(tmp)
        n = re.search('#\s+uid:\s+(\S+)',line)
        if n:
            uid = n.group(1)
        n = re.search('#\s+jobid:\s+(\S+)',line)
        if n:
            jobid = n.group(1)
        n = re.search('#\s+nprocs:\s+(\S+)',line)
        if n:
            nprocs = n.group(1)
        n = re.search('#\s+run\stime:\s+(\S+)',line)
        if n:
            runtime = n.group(1)
        n = re.search('#\s+metadata:\s+proj\s+=\s(\S+)',line)
        if n:
            proj = n.group(1)
        n = re.search('#\s+metadata(.*)proj=(\S+)',line)
        if n:
            proj = n.group(2)
        n = re.search('#\s+start_time_asci:\s+(\S+)\s+([a-zA-Z0-9_ :]+)',line)
        if n:
            start_time = n.group(2)
        n = re.search('#\s+end_time_asci:\s+(\S+)\s+([a-zA-Z0-9_ :]+)',line)
        if n:
            end_time = n.group(2)
            
    # add to Jobs_header tables
    s = 'SELECT * FROM jobs_info where logfilename ="'
    s += logfilename
    s += '"'
    affected_count = cur.execute(s)
    if (affected_count == 0):
        s = "INSERT IGNORE INTO jobs_info(logfilename, projid, uid, jobid, exe, nprocs, runtime, start_time,end_time,appname) VALUES('%s',%s,%s,%s,'%s',%s,%s,STR_TO_DATE('%s','%%b %%d %%T %%Y'),STR_TO_DATE('%s','%%b %%d %%T %%Y'),'%s')" % (logfilename, proj, uid,jobid,exe, nprocs, runtime, start_time, end_time, appname)
        cur.execute(s)



def parse_perf(cur,next_line_group,logfilename):
#             "total_bytes bigint, local_iotime float, local_meta float,"
#             "global_iotime float, global_meta float, shared_time_by_open float, shared_time_by_open_lastio float, shared_time_by_slowest float,"
 #            "agg_perf_by_cumul float, agg_perf_by_open float, agg_perf_by_open_lastio float, agg_perf_by_slowest float,"
 #            "iotime_by_cumul float, iotime_by_open float, iotime_by_open_lastio float, iotime_by_slowest float, iotime float,agg_perf_MB float, "
    total_bytes = 0
    agg_perf_by_cumul = 1
    agg_perf_by_open = 1
    agg_perf_by_open_lastio = 1
    agg_perf_by_slowest = 1
    iotime_by_cumul = 0
    iotime_by_open = 0
    iotime_by_open_lastio = 0
    iotime_by_slowest = 0
    local_iotime = 0
    local_meta = 0
    global_iotime = 0
    global_meta = 0
    shared_time_by_open = 0
    shared_time_by_open_lastio = 0
    shared_time_by_slowest =0

    for line in next_line_group:
        n = re.search('#\s+total_bytes:\s+(\S+)',line)
        if n:
			total_bytes = float(n.group(1))
        n = re.search('#\s+unique files: slowest_rank_time:\s+(\S+)',line)
        if n:
            local_iotime = float(n.group(1))
        n = re.search('#\s+unique files: slowest_rank_meta_time:\s+(\S+)',line)
        if n:
            local_meta = float(n.group(1))
        n = re.search('#\s+shared files: time_by_cumul_io_only:\s+(\S+)',line)
        if n:
            global_iotime = float(n.group(1))
        n = re.search('#\s+shared files: time_by_cumul_meta_only:\s+(\S+)',line)
        if n:
            global_meta = float(n.group(1))
        n = re.search('#\s+shared files: time_by_open:\s+(\S+)',line)
        if n:
            shared_time_by_open = float(n.group(1))
        n = re.search('#\s+shared files: time_by_open_lastio:\s+(\S+)',line)
        if n:
            shared_time_by_open_lastio = float(n.group(1))
        n = re.search('#\s+shared files: time_by_slowest:\s+(\S+)',line)
        if n:
            shared_time_by_slowest = float(n.group(1))
        n = re.search('#\s+agg_perf_by_cumul:\s+(\S+)',line)                       					
        if n:
            agg_perf_by_cumul = float(n.group(1))
        n = re.search('#\s+agg_perf_by_open:\s+(\S+)',line)                       					
        if n:
            agg_perf_by_open = float(n.group(1))
        n = re.search('#\s+agg_perf_by_open_lastio:\s+(\S+)',line)                       					
        if n:
            agg_perf_by_open_lastio = float(n.group(1))
        n = re.search('#\s+agg_perf_by_slowest:\s+(\S+)',line)                       					
        if n:
            agg_perf_by_slowest = float(n.group(1))

    if (agg_perf_by_cumul > 0):
        iotime_by_cumul = total_bytes/(1024*1024*agg_perf_by_cumul)
    if (agg_perf_by_open > 0):
        iotime_by_open = total_bytes/(1024*1024*agg_perf_by_open)
    if (agg_perf_by_open_lastio > 0):
        iotime_by_open_lastio = total_bytes/(1024*1024*agg_perf_by_open_lastio)
    if (agg_perf_by_slowest > 0):
        iotime_by_slowest = total_bytes/(1024*1024*agg_perf_by_slowest)        
        
    s = "Select runtime from jobs_info where logfilename='%s'" % (logfilename)
    cur.execute(s)
    row = cur.fetchone()
    runtime = row[0]
    if (max(iotime_by_cumul, iotime_by_slowest) < runtime):
        iotime = max(iotime_by_cumul, iotime_by_slowest)
    else:
        iotime = min(iotime_by_cumul, iotime_by_slowest)
    if (iotime > 0):
        agg_perf_MB = total_bytes/(iotime * 1024 * 1024) 
        

#             "total_bytes bigint, local_iotime float, local_meta float,"
#             "global_iotime float, global_meta float, shared_time_by_open float, shared_time_by_open_lastio float, shared_time_by_slowest float,"
 #            "agg_perf_by_cumul float, agg_perf_by_open float, agg_perf_by_open_lastio float, agg_perf_by_slowest float,"
 #            "iotime_by_cumul float, iotime_by_open float, iotime_by_open_lastio float, iotime_by_slowest float, iotime float,agg_perf_MB float, "

    s = 'update jobs_info set '
    s += 'total_bytes = ' + str(total_bytes) + ', local_iotime =' + str(local_iotime) + ', local_meta = ' + str(local_meta) + ',global_iotime =' + str(global_iotime)
    s += ', global_meta = ' + str(global_meta) + ',shared_time_by_open = ' + str(shared_time_by_open) + ', shared_time_by_open_lastio =' + str(shared_time_by_open_lastio)
    s += ', shared_time_by_slowest = ' + str(shared_time_by_slowest) + ',agg_perf_by_cumul = ' + str(agg_perf_by_cumul) + ',agg_perf_by_open = ' + str(agg_perf_by_open)
    s += ',agg_perf_by_open_lastio = ' + str(agg_perf_by_open_lastio) + ',agg_perf_by_slowest ='+ str(agg_perf_by_slowest) + ',iotime_by_cumul = ' + str(iotime_by_cumul)
    s += ',iotime_by_open = ' + str(iotime_by_open) + ',iotime_by_open_lastio = ' + str(iotime_by_open_lastio) + ',iotime_by_slowest = ' + str(iotime_by_slowest)
    s += ',iotime= ' + str(iotime) + ',agg_perf_MB = ' + str(agg_perf_MB)
    s += ' where logfilename = "' + logfilename + '"'
    #print s
    cur.execute(s)

def parse_files(cur,next_line_group,logfilename):
#             "total_count int, total_size bigint, total_max_offset bigint,"
#             "read_only_count int, read_only_size bigint, read_only_max_offset bigint,"
#             "write_only_count int, write_only_size bigint, write_only_max_offset bigint,"
#             "read_write_count int, read_write_size bigint, read_write_max_offset bigint,"
#             "unique_count int, unique_size bigint, unique_max_offset bigint,"
#             "shared_count int, shared_size bigint, shared_max_offset bigint,"
    for line in next_line_group:
        n = re.search('#\s+(\S+):\s+(\S+)\s+(\S+)\s+(\S+)',line)
        if n:
            counter = n.group(1)
            count = n.group(2)
            _bytes = n.group(3)
            max_offset = n.group(4)
            s = 'update jobs_info set '+ str(counter)+'_count'+ '=' + str(count) + ',' + str(counter) + '_size'+ '=' + str(_bytes) +  ',' + str(counter) +'_max_offset'+ '=' + str(max_offset) 
            s += ' where logfilename = "' + logfilename + '" '
#            s += str(counter)+'_count' + '=' + str(count) + ',' + str(counter) + '_size' + '=' + str(_bytes) + ',' + str(counter) + '_max_offset' + '=' + str(max_offset)
            #print s
            cur.execute(s)

def parse_file_list(cur,next_line_group,numfiles,nprocs):
    numpartshared = 0
    numallshared = 0
    numallshared_posix = 0
    numallshared_mpi = 0
    numpartshared_posix = 0
    numpartshared_mpi = 0
    numunique_posix = 0
    numunique_mpi = 0
    numallshared_posix_readwrite = 0
    numallshared_posix_read = 0
    numallshared_posix_write = 0
    numallshared_mpi_readwrite = 0
    numallshared_mpi_read = 0
    numallshared_mpi_write = 0
    numpartshared_posix_readwrite = 0
    numpartshared_posix_read = 0
    numpartshared_posix_write = 0
    numpartshared_mpi_readwrite = 0
    numpartshared_mpi_read = 0
    numpartshared_mpi_write = 0
    numunique_posix_readwrite = 0
    numunique_posix_read = 0
    numunique_posix_write = 0
    numunique_mpi_readwrite = 0
    numunique_mpi_read = 0
    numunique_mpi_write = 0

    if (next_line_group != ""):
        for each in next_line_group:
            m = re.search('(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)',each)
            if m:
                #print each
                mpi_posix = m.group(3)
                file_nprocs = int(m.group(4))
                read = float(m.group(8))
                write = float(m.group(9))
    #            print mpi_posix, file_nprocs, read, write

                if (file_nprocs == nprocs):
   #                print "allshared %s, %s\n" % (slowest,avg)
                    numallshared += 1
                    if (mpi_posix == "POSIX"):
                        numallshared_posix += 1
                        if ((read > 0) and (write > 0)):
                            numallshared_posix_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numallshared_posix_read += 1
                        elif ((write > 0) and (read == 0)):
                            numallshared_posix_write += 1

                    elif (mpi_posix == "MPI"):
                        numallshared_mpi += 1
                        if ((read > 0) and (write > 0)):
                            numallshared_mpi_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numallshared_mpi_read += 1
                        elif ((write > 0) and (read == 0)):
                            numallshared_mpi_write += 1

                elif ((file_nprocs > 1) and (file_nprocs < nprocs)):
                    numpartshared += 1
                    if (mpi_posix == "POSIX"):
                        numpartshared_posix += 1
                        if ((read > 0) and (write > 0)):
                            numpartshared_posix_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numpartshared_posix_read += 1
                        elif ((write > 0) and (read == 0)):
                            numpartshared_posix_write += 1

                    elif (mpi_posix == "MPI"):
                        numpartshared_mpi += 1
                        if ((read > 0) and (write > 0)):
                            numpartshared_mpi_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numpartshared_mpi_read += 1
                        elif ((write > 0) and (read == 0)):
                            numpartshared_mpi_write += 1
                else:
                    if (mpi_posix == "POSIX"):
                        numunique_posix += 1
                        if ((read > 0) and (write > 0)):
                            numunique_posix_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numunique_posix_read += 1
                        elif ((write > 0) and (read == 0)):
                            numunique_posix_write += 1

                    elif (mpi_posix == "MPI"):
                        numunique_mpi += 1
                        if ((read > 0) and (write > 0)):
                            numunique_mpi_readwrite += 1
                        elif ((write ==0) and (read > 0)):
                            numunique_mpi_read += 1
                        elif ((write > 0) and (read == 0)):
                            numunique_mpi_write += 1

    s = "Update jobs_info set partshared_count=%s, allshared_count=%s, allshared_posix_count=%s, allshared_mpi_count=%s, partshared_posix_count=%s, partshared_mpi_count=%s, unique_posix_count=%s, unique_mpi_count=%s, allshared_posix_readwrite=%s, allshared_posix_read=%s, allshared_posix_write=%s,allshared_mpi_readwrite=%s, allshared_mpi_read=%s, allshared_mpi_write=%s,partshared_posix_readwrite=%s, partshared_posix_read=%s, partshared_posix_write=%s,partshared_mpi_readwrite=%s, partshared_mpi_read=%s, partshared_mpi_write=%s,unique_posix_readwrite=%s, unique_posix_read=%s, unique_posix_write=%s, unique_mpi_readwrite=%s, unique_mpi_read=%s, unique_mpi_write=%s where logfilename='%s'" % (numpartshared, numallshared,numallshared_posix,numallshared_mpi,numpartshared_posix,numpartshared_mpi,numunique_posix,numunique_mpi,numallshared_posix_readwrite, numallshared_posix_read, numallshared_posix_write,numallshared_mpi_readwrite, numallshared_mpi_read, numallshared_mpi_write,numpartshared_posix_readwrite, numpartshared_posix_read, numpartshared_posix_write,numpartshared_mpi_readwrite, numpartshared_mpi_read, numpartshared_mpi_write,numunique_posix_readwrite, numunique_posix_read, numunique_posix_write, numunique_mpi_readwrite, numunique_mpi_read, numunique_mpi_write,logfilename)
    #print s
    cur.execute(s)

    #con.commit()
    #con.close()                                



def parse_header_perf_file(f):
    #global nprocs
    #global numfiles

    con = mdb.connect(database_dbhost, database_username, database_password,database_dbname)
    cur = con.cursor()
    # TODO: check if jobid exists. If yes, stop - already in the database
    s = 'select * from jobs_info where logfilename = "'
    s += str(logfilename)	
    s += '"'
    #print s
    affected_count = cur.execute(s)
    
    if affected_count:
        print "Job already added \n"
        sys.exit(0)  
   
    for line in f:
        n = re.search('# nprocs:\s+(\S+)',line)
        if n:
            nprocs = n.group(1)
        n = re.search('# total:\s+(\S+)\s+(.+)',line)
        if n:
            numfiles = n.group(1)
      
        # Extract header info, line starts with #
        n = re.search('#\s+size\sof\sjob\sstatistics:\s+(\S+)',line)
        if n:
            # Read the header - next 13 lines
            next_line_group = list(islice(f,13)) # there are 13 lines in this section
            if not next_line_group:
                print "no header info \n"
            else:
                parse_header(cur, next_line_group,logfilename)

        # Extract Perf info
        n = re.search('#\s+performance',line)
        if n:
            #next_line_group = list(islice(f,12))  # Old darshan-parser
            next_line_group = list(islice(f,24))    # New darshan-parser , there are 24 lines in this section
            if not next_line_group:
                print "no performance information \n" 
            else:
                parse_perf(cur, next_line_group,logfilename)


        # Extract Files info
        n = re.search('#\s+files',line)
        if n:
            next_line_group = list(islice(f,7))  # there are 7 lines in this section
            if not next_line_group:
                print "no files info \n"
            else:
                parse_files(cur,next_line_group,logfilename)
        
        # Extract File list detailed
        
        n = re.search('#\s+Per-file\s+summary(.+)detailed(.+)',line)
        if n:
            #numfiles = 0
            #s = "Select total_count,nprocs from jobs_info where logfilename='%s'" % (logfilename)
            #cur.execute(s)
            #row = cur.fetchone()
            #if (row == None):
            #    sys.exit()
            #else:
            #    numfiles = int(row[0])
            #    nprocs = int(row[1]) 
            next_line_group = list(islice(f,16,16+numfiles))  # there are 16 lines before the detailed information starts
            if not next_line_group:
                print "no files list detailed info \n"
            else:
                parse_file_list(cur,next_line_group,numfiles,nprocs)
 

    con.commit()
    con.close()
    
           
def parse_input_file():
    f = open(_InputFile, 'r')
    if (_mode == "0"):  # per_job information, just necessary columns
        parse_header_perf_file(f)
    elif (_mode == "1"):  # just per_file info
        parse_counter_from_file(f)
    else:
        print "Need correct mode for parsing counter"
        sys.exit(1)
    
                    
if __name__ == "__main__":
    parseArg(sys.argv[1:])
    conn = mdb.connect(database_dbhost, database_username, database_password, database_dbname)
    with conn:
        cur = conn.cursor()
        s = "show tables like 'jobs_info'"
        cur.execute(s)
        row = cur.fetchone()
        if (row == None):
            create_table()                            
    parse_input_file()
