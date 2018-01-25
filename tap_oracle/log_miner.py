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
 
plock = _thread.allocate_lock()
 
#set up our time window

# startYear = int(sys.argv[1].split('_')[0])
# startMonth = int(sys.argv[1].split('_')[1])
# startDay = int((sys.argv[1].split(' ')[0]).split('_')[2])
# startHour = int((sys.argv[1].split(' ')[1]).split('_')[4])
# startMinute = int((sys.argv[1].split(' ')[1]).split(':')[1])
# startTime=datetime.datetime(startYear, startMonth, startDay, startHour,startMinute, 0)

# endYear = int(sys.argv[2].split('_')[0])
# endMonth = int(sys.argv[2].split('_')[1])
# endDay = int((sys.argv[2].split(' ')[0]).split('_')[2])
# endHour = int((sys.argv[2].split(' ')[1]).split(':')[0])
# endMinute = int((sys.argv[2].split(' ')[1]).split(':')[1])
# endTime=datetime.datetime(endYear, endMonth, endDay, endHour, endMinute,0)
startYear = 2018
startMonth = 1
startDay = 23
startHour = 10
startMinute = 0
startTime=datetime.datetime(startYear, startMonth, startDay, startHour,startMinute, 0)

endYear = 2018
endMonth = 1
endDay = 28
endHour = 10
endMinute = 0
endTime=datetime.datetime(endYear, endMonth, endDay, endHour, endMinute,0)
 
#-----------------------------------------------------------------------
 
class readRedoThread(Thread):
  def __init__ (self,threadNum):
    Thread.__init__(self)
    self.t = threadNum
 
  def run(self):
 
    conn = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
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
            logStart.execute("begin sys.dbms_logmnr.start_logmnr(options => dbms_logmnr.dict_from_online_catalog + \
                                                                            dbms_logmnr.no_rowid_in_stmt); end;")
            
            # logStart.execute("describe v$logmnr_contents");
            # for description in logStart:
            #     print("Description:", description)

            contents.execute("select * from v$logmnr_contents where thread# = :1", [self.t])
            for change in contents:
                print("Change:", change)
                plock.acquire()
                plock.release()
        except cx_Oracle.DatabaseError as ex:
            print("Exception at row:", row, ex)


      minutes = datetime.timedelta(minutes=60)
      s = s + minutes
 
#-----------------------------------------------------------------------
 
# def restoreLogs():
#  #placeholder for future procedure to get any necessary archived redo
# logs from RMAN
#  pass
 
#-----------------------------------------------------------------------
 
threadList = []
threadNums = []
 
conn = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
cursor = conn.cursor()
cursor.execute("select distinct thread# from v$archived_log where first_time >= :1 and next_time <= :2",[startTime,endTime])
 
for row in cursor:
   threadNums.append(row[0])
   
#conn.close()
 
for i in threadNums:
    thisOne = readRedoThread(i)
    threadList.append(thisOne)
    thisOne.start()
 
for j in threadList:
 j.join()
