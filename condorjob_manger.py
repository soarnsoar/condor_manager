import os
import hashlib
import random
import string
import time

def random_string(length=16):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))
def ConvertStatusCode(_code):
    _code=int(_code)
    if _code==1:
        return 'Idle'
    elif _code==2:
        return 'Running'
    elif _code==3:
        return 'removing'
    elif _code==4:
        return 'Complete'
    elif _code==5:
        return "Held"
    elif _code==6:
        return "Transfer"
    elif _code==7:
        return "Suspended"

class manager:
    def __init__(self):        
        self.user=os.getenv("USER")
        self.GetJobInfo()

    def GetJobInfo(self):
        list_batchname=[]
        tempdir_name="temp/"+random_string()
        os.system('mkdir -p '+tempdir_name)
        #condor_q -json -attributes ClusterId,ProcId,JobBatchName,JobStatus,RequestMemory > filtered_jobs.json
        jobinfofile=tempdir_name+"/_current_job.txt"
        os.system("condor_q -json -attributes ClusterId,ProcId,JobBatchName,JobStatus,RequestMemory   > "+jobinfofile)
        f=open(jobinfofile)

        lines=f.readlines()
        lines_str="".join(lines)
        #print str(lines)
        exec("self.info="+str(lines_str))

        f.close()
        #os.system("rm "+jobinfofile)
        #os.system("rmdir "+tempdir_name)
        return self.info
        #JobBatchName
        #JobStatus
        #RequestMemory
        #ProcId
        #ClusterId
    def GetJid(self, this_info):
        return str(this_info["ClusterId"])+"."+str(this_info["ProcId"])

    def GetBatchName(self, this_info):
        return this_info["JobBatchName"]

    def GetMemory(self, this_info):
        return this_info["RequestMemory"]

    def GetStatus(self, this_info):
        status_code= this_info["JobStatus"]
        return status_code

    def ChangeMemory(self, this_info, _memory):
        ##condor_qedit <JobID> RequestMemory 
        _memory=str(int(_memory))
        jid=self.GetJid(this_info)
        command="condor_qedit "+jid+" RequestMemory "+_memory
        os.system(command)
    def ChangePrio(self, this_info, _prio):
        ##condor_qedit <JobID> RequestMemory 
        _prio=str(int(_prio))
        jid=self.GetJid(this_info)
        command="condor_prio -p "+_prio+" "+jid
        os.system(command)
    def ChangePrioUsingClusterId(self,_clusterid,_prio):
        _prio=str(_prio)
        _clusterid=str(_clusterid)
        os.system("condor_prio -p "+_prio+" "+_clusterid)
    def GetClusterIdFromJid(self,_jid):
        return _jid.split(".")[0]
if __name__ == '__main__':
    key="hadd"

    myjob=manager()

    for info in myjob.info:
        
        batchname=myjob.GetBatchName(info)
        jid = myjob.GetJid(info)
        if key in batchname:
            print batchname,jid
            myjob.ChangeMemory(info,60000)
            myjob.ChangePrio(info,1)
