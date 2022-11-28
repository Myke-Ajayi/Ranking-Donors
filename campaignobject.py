#creating a class for retrieving sql data

import sqlite3
import csv

class databasequery:

    dblocation = None
    connection = None
    #"/Users/king_myke/Desktop/MaxRose/max_rose.db"
    
    def __init__ (self, dblocation="none"):
        self.dblocation = dblocation
        self.connection = sqlite3.connect(self.dblocation)
        
    def dropdb(self):
        self.connection.close()


class campaign:

    candidate = None
    cmte_id = None
    primaryContributions = None
    generalContributions = None
    targetGContributions = None
    combinedContributors = None
    scoredContributors = None
    actblue = None
    fec_table = None
    nyc_sales_table = None
    contributoraddresses= None
    limit = 800
    addresslist = None
    addressDictWithPrices = None
    otherContributions = None
    rankedOccupations = None
    rankedJobList = None
    scoredJobDict = None
    orderedContributors = []
    
    def __init__(self,cmte_id,candidate):
        self.cmte_id=cmte_id
        self.candidate=candidate
        self.actblue = 'C00796540'
        self.fec_table = "FEC_DATA_2021_2022"
        self.nyc_sales_table = "NYC_SALES_DATA"
        self.contributor_addresses = "contributoraddresses"
        self.primaryContributions = {}
        self.generalContributions = {}
        self.addressDictWithPrices = {}
        self.addresslist = []
        self.targetGContributions = {}
        self.combinedContributors = {}
        self.scoredContributors = {}
        self.otherContributions = {}
        self.rankedOccupations = {}
        self.rankedJobList = []
        self.scoredJobDict = {}
        self.orderedContributors = []
        print(cmte_id)
        print(candidate)

    def getContributions(self):
        ##need to initialize database before using
        d = databasequery("/Users/king_myke/Desktop/MaxRose/max_rose.db")
        cursor = d.connection.cursor()
        cursor.execute("SELECT NAME, TRANSACTION_AMT FROM FEC_DATA_2021_2022 WHERE CMTE_ID=? AND TRANSACTION_PGI=? LIMIT ?",(self.cmte_id,"P",self.limit))
        temp = cursor.fetchall()
        for t in temp:
            tname, ttransaction = t
            #this sum of donations works for max limit as later donations are missed otherwise
            if tname in self.primaryContributions:
                self.primaryContributions[tname] = self.primaryContributions.get(tname) + ttransaction
            else:    
                self.primaryContributions[tname] = ttransaction
        cursor.execute("SELECT NAME, TRANSACTION_AMT FROM FEC_DATA_2021_2022 WHERE CMTE_ID=? AND TRANSACTION_PGI=? LIMIT ?",(self.cmte_id,"G",self.limit))
        temp = cursor.fetchall()
        for t in temp:
            tname, ttransaction = t
            if tname in self.generalContributions:
                self.generalContributions[tname] = self.generalContributions.get(tname) + ttransaction
            else:    
                self.generalContributions[tname] = ttransaction
        #print(self.primaryContributions.get("MARGOLIS, JONATHAN")) // debug line
        d.dropdb()

    # combines primary and general contributions by person
    def combineContributorList(self):
        for t in self.primaryContributions:
            self.combinedContributors[t]=[self.primaryContributions.get(t),0]
        for t in self.generalContributions:
            if t in self.combinedContributors:
                pamt, gamt = self.combinedContributors.get(t)
                self.combinedContributors[t]=[pamt,self.generalContributions.get(t)]
            else:
                self.combinedContributors[t]=[0,self.generalContributions.get(t)]

        
    # returns filtered dictionary of name, donations
    def donatedtoprimarybutnotgeneral(self,min,max):
        for n in self.combinedContributors:
            if self.combinedContributors.get(n)[1] < self.combinedContributors.get(n)[0]:
                self.targetGContributions[n] = self.primaryContributions.get(n)
                d = {k:v for (k,v) in self.targetGContributions.items() if v > min and v<=max}
        return(d)

    #seeing how much people donated elsewhere
    def pullothercontributions(self):
        d = databasequery("/Users/king_myke/Desktop/MaxRose/max_rose.db")
        cursor = d.connection.cursor()
        temp = cursor.execute("SELECT NAME, ZIPCODE FROM FEC_DATA_2021_2022 WHERE CMTE_ID=? LIMIT ?",(self.cmte_id,self.limit))
        templist = temp.fetchall()
        for c in templist:
            cname, czipcode = c
            temp2 = cursor.execute("SELECT NAME, TRANSACTION_AMT FROM FEC_DATA_2021_2022 WHERE CMTE_ID !=? AND CMTE_ID !=? AND NAME=? AND ZIPCODE=?",(self.cmte_id,self.actblue,cname,czipcode))
            others = temp2.fetchall()
            if(len(others) != 0):
                totalmoney = 0
                for m in others:
                    totalmoney = totalmoney + int(m[1])
                self.otherContributions[cname] = totalmoney
        d.dropdb()
        return(self.otherContributions)
    
    # returns dictionary nonzero values of name, address w/price
    def addressPrices(self):
        # need to update to use block/lot/apt name rather than only address line
        d = databasequery("/Users/king_myke/Desktop/MaxRose/max_rose.db")
        cursor = d.connection.cursor()
        cursor.execute("SELECT NAME, ADDRESS, APT FROM contributoraddresses LIMIT ?",(self.limit,)) 
        self.addresslist = cursor.fetchall()
        for a in self.addresslist:
            #print(aname, aaddress, aapt)
            aname, aaddress, aapt = a
            cursor.execute("SELECT ADDRESS, apt, SALEPRICE, SALEDATE from NYC_SALES_DATA WHERE ADDRESS LIKE ? ",(aaddress+'%',))
            #get most recent price point / correct apt number
            allprices = cursor.fetchall()
            sortedPrices = sorted(allprices, key=lambda x:x[3], reverse=True)
            #sorting by date doesnt work correctly this way - thinks string is text not date)
            if bool(sortedPrices):
                foundnewprice = 0
                p = 0
                numofprices = len(sortedPrices)
                while (foundnewprice == 0 and numofprices > p):
                    s = sortedPrices[p]
                    if not (isinstance(s[2], int) or isinstance(s[2], float)):
                        if any(aapt in t for t in s):
                            if (isinstance(s[2], int) or isinstance(s[2], float)):
                                self.addressDictWithPrices[aname] = [aaddress,s[2]]
                                foundnewprice = s[2]
                            else:
                                firstprice = s[2]
                                price1 = firstprice.strip('$')
                                price2 = price1.strip('\t')
                                numprice = price2.replace(",","")
                                self.addressDictWithPrices[aname] = [aaddress,numprice]
                                foundnewprice= numprice
                    p = p+1          
        d.dropdb()
        return(self.addressDictWithPrices)
        # very dependent on people entering accurate addresses - see Patricia Allessio
        # date sorting does it by 2 digit year then 4 digit yeat
              
    def rankOccupations(self):
        d = databasequery("/Users/king_myke/Desktop/MaxRose/max_rose.db")
        cursor = d.connection.cursor()
        cursor.execute("SELECT OCCUPATION, TRANSACTION_AMT FROM FEC_DATA_2021_2022 WHERE CMTE_ID =? LIMIT ?",(self.cmte_id,self.limit,))
        joblist = cursor.fetchall()
        for job in joblist:
            occ, amt = job
            j = 0
            if occ not in self.rankedOccupations:
                self.rankedOccupations[occ] = [amt,1]
            else:
                j = self.rankedOccupations.get(occ)[1]+1 # incrementing count of occupation
                self.rankedOccupations[occ] = [self.rankedOccupations.get(occ)[0]+amt,j] # [0] is the amt donated
        for occ in self.rankedOccupations:
            totalamt, jobcount = self.rankedOccupations.get(occ)
            self.rankedOccupations[occ] = totalamt/jobcount
        numberofjobs = len(self.rankedOccupations)
        self.rankedJobList = sorted((amt,job) for (job,amt) in self.rankedOccupations.items())
        d.dropdb()
        return(self.rankedJobList)


    
    def scoreContributors(self):
        # needs to handle starting dictionary fresh then always checking if previously included

        # incrementing score for job category
        jobscore = 0
        l = len(self.rankedJobList)
        k = 1
        for e in self.rankedJobList: # ranked list by job of donations
            if k/l <= .25:
                jobscore = jobscore -1
            elif k/l <= .5:
                jobscore = jobscore +0
            elif k/l <= .75:
                jobscore = jobscore +1
            else:    
                jobscore = jobscore +3
            k = k+1
            occ = e[1]
            self.scoredJobDict[e[1]] = jobscore
            jobscore = 0
        d = databasequery("/Users/king_myke/Desktop/MaxRose/max_rose.db")
        cursor = d.connection.cursor()
        cursor.execute("SELECT NAME, OCCUPATION FROM FEC_DATA_2021_2022 WHERE CMTE_ID =? LIMIT ?",(self.cmte_id,self.limit,))
        namejoblist = cursor.fetchall()
        for n in namejoblist:
            name, job = n
            jobscore = self.scoredJobDict.get(job)
            self.scoredContributors[name] = jobscore
        print("incrementing for jobs")
        d.dropdb()
        
        #incrementing score for donating in primary but not general
        personalscore = 0
        tempdict = self.donatedtoprimarybutnotgeneral(250,2700)
        for person in tempdict:
            if tempdict.get(person) > 2000:
                personalscore += 3
            elif tempdict.get(person) > 1000:
                personalscore += 2
            else:
                personalscore += 1
            if person not in self.scoredContributors:
                self.scoredContributors[person] = personalscore
            else:
                self.scoredContributors[person] = self.scoredContributors.get(person) + personalscore
            personalscore = 0
        print("incrementing for donating in primary")

        # incrementing score for owning home
        personalscore = 0
        tempdict2 = self.addressPrices()
        for person in tempdict2:
            if float(tempdict2.get(person)[1]) > 2000000:
                personalscore = personalscore +3
            elif float(tempdict2.get(person)[1]) > 1000000:
                personalscore = personalscore +2
            else:
                personalscore = personalscore +1
            if person not in self.scoredContributors:
                self.scoredContributors[person] = personalscore
            else:
                self.scoredContributors[person] = self.scoredContributors.get(person) + personalscore
            personalscore = 0
        print("incrementing for owning a home")

        # incrementing score for donating to other campaigns
        personalscore = 0
        tempdict3 = self.pullothercontributions()
        for person in tempdict3:
            if tempdict3.get(person) > 10000:
                personalscore = personalscore + 3
            elif tempdict3.get(person) > 2700:
                personalscore = personalscore + 2
            elif tempdict3.get(person) > 250:
                personalscore = personalscore + 1
            if person not in self.scoredContributors:
                self.scoredContributors[person] = personalscore
            else:
                self.scoredContributors[person] = self.scoredContributors.get(person) + personalscore
            personalscore = 0
        print("incrementing for donating to others")

        ## need to increment for value of network
        
        ## sorting in order   
        self.sortedContributors = sorted(((score,name) for (name,score) in self.scoredContributors.items()), reverse=True)

    def createCSV(self, fname):
        with open(fname,'w') as output:
            csvwriter = csv.writer(output)
            for a in self.sortedContributors:
                csvwriter.writerow(a)
    
        
c = campaign("C00652248","MaxRose")
c.getContributions()
print("getting contributions")
c.combineContributorList()
print("combining lists")
c.donatedtoprimarybutnotgeneral(1000,2700)
print("checking primary vs general")
c.addressPrices()
print("checking addresses")
c.pullothercontributions()
print("checking other donations")
c.rankOccupations()
print("checking jobs")
c.scoreContributors()
print("viola")
c.createCSV("/Users/king_myke/Desktop/MaxRose/test.csv")
