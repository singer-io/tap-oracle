#!/home/oracle/local/bin/python
"""
------------------------------------------------------------------------
Author:         Steve Howard
Date:           December 10, 2010
Purpose:        Simple program to process redo log changes
------------------------------------------------------------------------
"""
 
import cx_Oracle, sys, string, _thread, datetime
from threading import Lock
from threading import Thread
from singer import utils
 
plock = _thread.allocate_lock()
 
startYear = 2018
startMonth = 1
startDay = 23
startHour = 10
startMinute = 0
startTime=datetime.datetime(startYear, startMonth, startDay, startHour,startMinute, 0)

endYear = 2018
endMonth = 2
endDay = 1
endHour = 23
endMinute = 0
endTime=datetime.datetime(endYear, endMonth, endDay, endHour, endMinute,0)
 
#-----------------------------------------------------------------------
 
class readRedoThread(Thread):
  def __init__ (self,threadNum):
    Thread.__init__(self)
    self.t = threadNum
 
  def run(self):
    #dsn = cx_Oracle.makedsn('127.0.0.1', 1521, 'ORCL') 
    conn = cx_Oracle.connect('root', 'BiouTaSeCtOmPavA', '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SID=ORCL)))')
    cursor = conn.cursor()
    contents = conn.cursor()
 
    cursor.prepare("select name \
                      from v$archived_log \
                      where first_time between :1 and :2 + 60/1440 \
                        and thread# = :3 \
                        and deleted = 'NO' \
                        and name is not null \
                        and dest_id = 1")
 
    #...and loop until we are past the ending time in which we are interested...
    global startTime
    global endTime
 
    s = startTime
    e = endTime

    while s < e:

      cursor.execute("select name \
                        from v$archived_log \
                        where first_time between :1 and :2 + 60/1440 \
                          and thread# = :3 \
                          and deleted = 'NO' \
                          and name is not null \
                          and dest_id = 1",[s, s, self.t])
      for row in cursor:
        print("Row:", row)
        logAdd = conn.cursor()
        try:
            logAdd.execute("begin sys.dbms_logmnr.add_logfile(:1); end;",[row[0]])
            logStart = conn.cursor()

            #you may have to use an "offline" catalog if this is being run 
            #  against a standby database, or against a read-only database.
            #logStart.execute("begin sys.dbms_logmnr.start_logmnr(dictfilename => :1); end;",["/tmp/dictionary.ora"])
            #logStart.execute("begin sys.dbms_logmnr.start_logmnr(options => dbms_logmnr.dict_from_online_catalog \
                                                                           # dbms_logmnr.print_pretty_sql \
                                                                           # dbms_logmnr.no_rowid_in_stmt); end;")

            try:
                logStart.execute("begin sys.dbms_logmnr.start_logmnr(options => dbms_logmnr.dict_from_online_catalog + \
                                                                                dbms_logmnr.no_rowid_in_stmt); end;")


                #contents.execute("select scn, sql_redo, table_name, operation, seg_type_name from v$logmnr_contents where thread# = :1", [self.t])
                contents.execute("select sql_redo, table_name from v$logmnr_contents where thread# = :1", [self.t])
                
                for change in contents:
                    plock.acquire()
                    print("SQL redo:", change[0])
                    #contents.execute("begin sys.dbms_logmnr.mine_value(:1, :2); end;", change[0], change[1])
                    # for result in contents:
                    #   print("results:", result)
                    print("SCN:", change[0])
                    print("sql redo:", change[1])
                    # print("table name", change[2])
                    # print("operation", change[3])
                    # print("seg_type_name", change[4])
                    plock.release()

            except:
                #print("Something bad happened:")
                pass

        except cx_Oracle.DatabaseError as ex:
            pass
            #print("Exception at row:", row, ex)


      minutes = datetime.timedelta(minutes=60)
      s = s + minutes
 
#-----------------------------------------------------------------------
 
# def restoreLogs():
#  #placeholder for future procedure to get any necessary archived redo
# logs from RMAN
#  pass
 
#-----------------------------------------------------------------------

def get_logs(config):
    threadList = []
    threadNums = []
    global startTime
    global endTime
    #startTime = utils.strptime(config["start_date"])
    #endTime = datetime.datetime.now()
    #print(startTime)

    conn = cx_Oracle.connect(config["user"], config["password"], cx_Oracle.makedsn(config["host"], config["port"], 'ORCL'))
    #conn = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
    cursor = conn.cursor()
    cursor.execute("select distinct thread# from v$archived_log where first_time >= :1 and next_time <= :2",[startTime,endTime])

    for row in cursor:
       threadNums.append(row[0])

    for i in threadNums:
        thisOne = readRedoThread(i)
        threadList.append(thisOne)
        thisOne.start()

    for j in threadList:
     j.join()
