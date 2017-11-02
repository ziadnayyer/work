# # # coding=utf-8
# # import logging
# # import psutil
# # import socket
# # from datetime import datetime, timedelta
# # import uuid
# # import locale
# import MySQLdb
# # import time
# # import os
# # import string
# # from flask import render_template, request, session, jsonify, Response, Blueprint, current_app, g
# # from werkzeug.local import LocalProxy
# # from psdash.helpers import socket_families, socket_types
# # from distutils.log import info
import MySQLdb
import threading
import os

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
        user="root",         # your username
        passwd="Gift123",  # your password
        db="test")        # name of the data base
curl = db.cursor()

dbr = 'test'
dbr_user = 'root'
dbr_password = 'Gift123'

def update():
    curl.execute("SELECT ip_address FROM resource")
    results = curl.fetchall()
    #print results
    
    threading.Timer(10.0, update).start()
    crop_results = iter(results)
    next(crop_results)
    for n in crop_results:
        status = True if os.system("ping -c 1 "  + n[0]) is 0 else False       
        if status == True:
            #print n[0]
            dbr_host = n[0]
            #print dbr_host
            conn = MySQLdb.Connection(db=dbr, host=dbr_host, user=dbr_user, passwd=dbr_password)
            curr = conn.cursor()
            curr.execute("SELECT a.ip_address, a.cloudlet_name, b.status, b.decision, c.* from resource a, status b, user_request c")
            results = curr.fetchall()
            for row in results:
                cloudlet_ip = row[0]
                cloudlet_name = row[1]
                request_status = row[2]
                request_decision = row[3]
                request_number = row[4]
                user = row[5]
                vm_ip = row[6]
                vm_cpu = row[7]
                vm_storage = row[8]
                vm_memory = row[9]
                vm_user = row[10]
                vm_pass = row[11]
                #print request_number
                #print "Retrieval Successful"
                 
                curl.execute("select request_number, ip_address from user_request")
                result = curl.fetchall()
                for i in range(len(result)):
                    if (result[i][0] == request_number and result[i][1]== cloudlet_ip):
                        record = 1
                    else:
                        record = 0
                #print record
                if record == 0:
                    curl.execute("INSERT INTO user_request SET ip_address='%s',cloudlet_name='%s',status='%s',decision='%s',request_number=%s,user='%s',vm_ip='%s',vm_cpu=%s,vm_storage=%s,vm_memory=%s,vm_user='%s',vm_pass='%s'"% (cloudlet_ip, cloudlet_name, request_status, request_decision, request_number, user, vm_ip, vm_cpu, vm_storage, vm_memory, vm_user, vm_pass))
                    db.commit()
                    print "Insertion Successful"
#                 else:   
#                     print "Record already exist"
        else:
            print "connection failed", n[0]
    

#     curl.execute("SELECT * FROM user_request")
#     results = curl.fetchall()
#     
#     widths = []
#     columns = []
#     tavnit = '|'
#     separator = '+' 
#     
#     for cd in curl.description:
#         widths.append(max(cd[2], len(cd[0])))
#         columns.append(cd[0])
#     
#     for w in widths:
#         tavnit += " %-"+"%ss |" % (w,)
#         separator += '-'*w + '--+'
#     
#     print(separator)
#     print(tavnit % tuple(columns))
#     print(separator)
#     for row in results:
#         print(tavnit % row)
#     print(separator)
    
    decision = 'pending'
    curl.execute("SELECT * FROM user_request where decision='%s'"% (decision))
    rqresult = curl.fetchall()
    #print rqresult
    curl.execute("delete from el_cloudlet")
    for row in rqresult:
        cloudlet_ip = row[0]
        cloudlet_name = row[1]
        request_status = row[2]
        request_decision = row[3]
        request_number = row[4]
        user = row[5]
        vm_ip = row[6]
        vm_cpu = row[7]
        vm_storage = row[8]
        vm_memory = row[9]
        vm_user = row[10]
        vm_pass = row[11]
    
        curl.execute("select ip_address,cpu_cores,disk_free,memory_free from resource")
        rresult = curl.fetchall()
        #print rresult
        el_cloudlet=[]
        
        for i in range(len(rresult)):
            if (rresult[i][1] >= vm_cpu and rresult[i][2] >= vm_storage and rresult[i][3] >= vm_memory):
                record = 1
                #print request_number,record,rresult[i][0]
                el_cloudlet.append(rresult[i][0])
            else:
                record = 0
                el_cloudlet.append('NULL')
                #print request_number,record,rresult[i][0]
        #print request_number,el_cloudlet
        
        for i in range(len(el_cloudlet)):
            #if el_cloudlet[i] != 'NULL':
            curl.execute("INSERT INTO el_cloudlet SET cloudlet_ip='%s',request_number=%s,el_cloudlet_ip='%s'"% (cloudlet_ip,request_number,el_cloudlet[i]))
            db.commit()
            #print el_cloudlet
            #else:
            #    print "----"
        
        
update()
  
  


