import os
import hashlib
import random
import string
import time

def random_string(length=16):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


class manager:
    def __init__(self):        
        self.user=os.getenv("USER")
        self.GetJidDict()

    def GetListOfBatchName(self):
        list_batchname=[]
        tempdir_name="temp/"+random_string()
        os.system('mkdir -p '+tempdir_name)
        jobinfofile=tempdir_name+"/_current_job.txt"
        os.system("condor_q "+self.user+" > "+jobinfofile)
        f=open(jobinfofile)

        lines=f.readlines()
        
        for line in lines:
            #print line
            #print user
            if line.startswith(self.user):
                batchname=line.split()[1]
                #jid=line.split()[-1].split(".")[0]
                list_batchname.append(batchname)

        f.close()
        os.system("rm "+jobinfofile)
        os.system("rmdir "+tempdir_name)
        return list(set(list_batchname))

    def GetJidWithBatchName(self,batchname):
        #condor_q -nobatch -constraint "JobBatchName == \"$1\""
        tempdir_name="temp/"+random_string()
        os.system('mkdir -p '+tempdir_name)
        jobinfofile=tempdir_name+"/_current_job_"+batchname+".txt"

        listing_command="condor_q -nobatch -constraint \'JobBatchName ==\""+batchname+"\"\'"
        #print listing_command
        os.system(listing_command+ ">" +jobinfofile)
        f=open(jobinfofile)

        lines=f.readlines()
        
        list_jid_status=[]
        for line in lines:
            if len(line.split())<2:continue
            #print line
            second_row=line.split()[1] ##expected to be username
            first_row=line.split()[0] ##expected to be jid
            #print first_row,second_row
            if second_row==self.user:
                jid=first_row
                jobstatus=line.split()[5]
                list_jid_status.append((jid,jobstatus))
        f.close()
        os.system("rm "+jobinfofile)
        os.system("rmdir "+tempdir_name)
        return list_jid_status


    def GetJidDict(self):
        print "---Collect job infos----"
        os.system("date")
        batchnames=self.GetListOfBatchName()
        self._mydict={}
        for batchname in batchnames:
            print "Read...->",batchname
            self._mydict[batchname]=self.GetJidWithBatchName(batchname)
            time.sleep(0.5)
        print "---[DONE]Collect job infos---"
    def GetJidWithKeyword(self,keyword):
        this_list=[]
        for batchname in self._mydict:
            if keyword in batchname:
                print "Collect jid of ->",batchname
                for this_job_jid_status in self._mydict[batchname]:
                    this_job_jid=this_job_jid_status[0]
                    this_list.append(this_job_jid)

        return this_list
    def ChangeMemory(self,_jid,_memory):
        _jid=str(_jid)
        _memory=str(int(_memory))
        command="condor_qedit "+_jid+" RequestMemory "+_memory
        os.system(command)
        time.sleep(0.5)
    def ChangePrio(self,_jid,_prio):
        _jid=str(_jid)
        _prio=str(int(_prio))
        command="condor_prio -p "+_prio+" "+_jid
        print command
        os.system(command)
        time.sleep(0.5)
if __name__ == '__main__':
    True
    myjob=manager()
    jidlist=myjob.GetJidWithKeyword("hadd")
    print jidlist
    for jid in jidlist:
        True
        #print
        myjob.ChangeMemory(jid,60000)
        #myjob.ChangePrio(jid,1)
