# coding=utf-8
# import logging
# import psutil
# import socket
# from datetime import datetime, timedelta
# import uuid
# import locale
# import time
# import string
# from flask import render_template, request, session, jsonify, Response, Blueprint, current_app, g
# from werkzeug.local import LocalProxy
# from psdash.helpers import socket_families, socket_types
# from distutils.log import info
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
    
    ## Get requests from cloudlets and decision parameters
    self = '172.16.24.34'
    curl.execute("SELECT ip_address FROM resource where ip_address!='%s'"% (self))
    results = curl.fetchall()
    #print results
    
    threading.Timer(10.0, update).start()
    
    ##used crop or iter next to skip itself 172.16.24.34 (Broker)
    #crop_results = iter(results)
    #next(crop_results)
    for n in results:
        #print n[0]
        status = True if os.system("ping -c 1 "  + n[0]) is 0 else False       
        if status == True:
            #print n[0]
            dbr_host = n[0]
            #print dbr_host
            conn = MySQLdb.Connection(db=dbr, host=dbr_host, user=dbr_user, passwd=dbr_password)
            curr = conn.cursor()
            curr.execute("SELECT a.ip_address, a.cloudlet_name, b.status, b.decision, c.*, d.hopcount, d.resource_index from resource a, status b, user_request c, decision_parameters d")
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
                hopcount = row[12]
                resource_index = row[13]
                #print resource_index
                #print "Retrieval Successful"
                 
                req_record_exist = curl.execute("select request_number, ip_address from user_request")
                req_result = curl.fetchall()
                
                dec_param_exist = curl.execute("select * from decision_parameters where cloudlet_ip='%s'"% (cloudlet_ip))
                dec_result = curl.fetchall()
                #print result
                
                #ziad# check if decision parameter table is empty
                #ziad# and for all cloudlet the record exist and updated
                if dec_param_exist != 0:
                    for i in range(len(dec_result)):
                        if (dec_result[i][0] == cloudlet_ip):
                            record = 1
                            break
                        else:
                            record = 0
                            #print record
                    if record == 0:
                        curl.execute("INSERT INTO decision_parameters SET cloudlet_ip='%s',hopcount=%s,resource_index=%s"% (cloudlet_ip,hopcount,resource_index))
                        db.commit()
                        #print "Insertion Successful 2"
                    else:
                        curl.execute("update decision_parameters SET hopcount=%s,resource_index=%s where cloudlet_ip='%s'"% (hopcount,resource_index,cloudlet_ip))
                        db.commit()
                else:   
                    curl.execute("INSERT INTO decision_parameters SET cloudlet_ip='%s',hopcount=%s,resource_index=%s"% (cloudlet_ip,hopcount,resource_index))
                    db.commit()
                    #print "Insertion Successful 1"
                
                #ziad# check if there is any new request at any cloudlet
                if req_record_exist != 0:
                    for i in range(len(req_result)):
                        if (req_result[i][0] == request_number and req_result[i][1]== cloudlet_ip):
                            record = 1
                            break
                        else:
                            record = 0
                #print record
                    if record == 0:
                        curl.execute("INSERT INTO user_request SET ip_address='%s',cloudlet_name='%s',status='%s',decision='%s',request_number=%s,user='%s',vm_ip='%s',vm_cpu=%s,vm_storage=%s,vm_memory=%s,vm_user='%s',vm_pass='%s'"% (cloudlet_ip, cloudlet_name, request_status, request_decision, request_number, user, vm_ip, vm_cpu, vm_storage, vm_memory, vm_user, vm_pass))
                        db.commit()
                        #print "Insertion Successful 2"
                    else:
                        curl.execute("update decision_parameters SET hopcount=%s,resource_index=%s where cloudlet_ip='%s'"% (hopcount,resource_index,cloudlet_ip))
                        db.commit()
                else:   
                    curl.execute("INSERT INTO user_request SET ip_address='%s',cloudlet_name='%s',status='%s',decision='%s',request_number=%s,user='%s',vm_ip='%s',vm_cpu=%s,vm_storage=%s,vm_memory=%s,vm_user='%s',vm_pass='%s'"% (cloudlet_ip, cloudlet_name, request_status, request_decision, request_number, user, vm_ip, vm_cpu, vm_storage, vm_memory, vm_user, vm_pass))
                    db.commit()
                    #print "Insertion Successful 1"
                  
        else:
            print "Remote request and decision parameters not received, connection failed with", n[0]
    
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
    

    ## Select request with pending decision and filter eligible cloudlets
    
    decision = 'pending'
    curl.execute("SELECT * FROM user_request where decision='%s'"% (decision))
    rqresult = curl.fetchall()
    #print rqresult
    #curl.execute("delete from el_cloudlet")
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
    
        curl.execute("select ip_address,cpu_cores,disk_free,memory_free from resource where ip_address!='%s'"% (self))
        rresult = curl.fetchall()
        #print rresult
        
        el_cloudlet = []
        
        ## Filter eligible cloudlets
        
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
         
        ##Stop duplicate processing on request for eligible cloudlets
        ## and insert eligible cloudlets in table el_cloudlets
         
        rec_exist = curl.execute("select cloudlet_ip,request_number from el_cloudlet")
        el_result = curl.fetchall()
        #print el_result
        if rec_exist != 0:
            #print el_result[1][0]
            for i in range(len(el_result)):
                #print request_number, el_result[i][0]
                if cloudlet_ip == el_result[i][0] and request_number == el_result[i][1]:
                    #print "case 1",result[i][0]
                    record = 1
                    break
                else:
                    record = 0
                    #print "case 0", result[i][0]
                   
            #print record
            if record == 0:
                for i in range(len(el_cloudlet)):
                    curl.execute("INSERT INTO el_cloudlet SET cloudlet_ip='%s',request_number=%s,el_cloudlet_ip='%s'"% (cloudlet_ip,request_number,el_cloudlet[i]))
                    db.commit()
                    #print "Insertion Successful 2"
                    #print record
        else:
            #print "Table Empty"
            for i in range(len(el_cloudlet)):
                curl.execute("INSERT INTO el_cloudlet SET cloudlet_ip='%s',request_number=%s,el_cloudlet_ip='%s'"% (cloudlet_ip,request_number,el_cloudlet[i]))
                db.commit()
                #print "Insertion Successful 1"
        
        # Finalize decision, update pending decision status in user_request
        # and push the decistion along status to the respective cloudlet
        #print request_number
        el_exist = curl.execute("select cloudlet_ip,el_cloudlet_ip from el_cloudlet where el_cloudlet_ip!='NULL' and request_number=%s"% (request_number))
        el_result = curl.fetchall()
        rindex = []               
        if el_exist != 0: 
            for n in el_result:
                cloudlet_ip = n[0]
                el_cloudlet_ip = n[1]
                
                curl.execute("update user_request set decision='completed' where ip_address='%s' and request_number=%s"% (cloudlet_ip,request_number))
                db.commit()
                 
                curl.execute("select * from decision_parameters where cloudlet_ip='%s'"% (el_cloudlet_ip))
                dp_result = curl.fetchall()
                #print dp_result
                
                rindex.append(dp_result[0][2])
                #print dp_result[0][2]
                #print rindex
            #print max(rindex),request_number
            exist = curl.execute("select * from decision")
            res = curl.fetchall()
            #print res[0][0]
            if exist !=0:
                for n in res:
                    rqn = n[0]
                    srcip = n[1]
                    dstip = n[2]
                    if (srcip == cloudlet_ip and rqn == request_number):
                        record = 1
                        break
                    else:
                        record = 0
                        
                if record == 0:
                    curl.execute("insert into decision set request_number=%s,src_cl_ip='%s',dst_cl_ip='%s'"% (request_number, cloudlet_ip, el_cloudlet_ip))
                    db.commit()        
            else:
                curl.execute("insert into decision set request_number=%s,src_cl_ip='%s',dst_cl_ip='%s'"% (request_number, cloudlet_ip, el_cloudlet_ip))
                db.commit() 
        
        else:
            print "No eligible cloudlet exist"             
        
            
    #ziad# Send decision back to source cloudlet
    dexist = curl.execute("select * from decision")
    ldecision = curl.fetchall()    
    if dexist !=0:
        for row in ldecision:
            request_number = row[0]
            src_cloudlet_ip = row[1]
            dst_cloudlet_ip = row[2]    
                
            status = True if os.system("ping -c 1 "  + src_cloudlet_ip) is 0 else False       
            if status == True:
                conn = MySQLdb.Connection(db=dbr, host=src_cloudlet_ip, user=dbr_user, passwd=dbr_password)
                curr = conn.cursor()
            
                rdec_exist = curr.execute("select * from decision")
                rdec_result = curr.fetchall()
                #print rdec_exist
                if rdec_exist != 0:
                    for i in range(len(rdec_result)):
                        if (rdec_result[i][0] == request_number):
                            record = 1
                            break
                        else:
                            record = 0
                            #print record
                    if record == 0:
                        curr.execute("insert into decision set request_number=%s,dst_cl_ip='%s'"% (request_number, dst_cloudlet_ip))
                        conn.commit()
                        #print "Insertion Successful 2"
                else:   
                    curr.execute("insert into decision set request_number=%s,dst_cl_ip='%s'"% (request_number, dst_cloudlet_ip))
                    conn.commit()
                    #print "Insertion Successful 1"
            else:
                print "Decision was not sent to source cloudlet, connection failed with", src_cloudlet_ip                               
    else:
        print "No decision sending is pending"
        
update()
  
  


