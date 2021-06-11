# ------------------------ Description ---------------------------------

# Get all TS ne/Te profile for a certain day
# Special verion: 1 file per trigger
# Indent = tab

# ------------------------ Change log ----------------------------------

# 2018-02-27 / JS: Write("\n") moved into loop, each measurement separated by line break

# ------------------------ To do ---------------------------------------

# Get Thomson measurement location: @ Target or @ Source

# ------------------------ Imports -------------------------------------

import magnum.comm.main.settings as settings
import codac.datastore.client as client
import datetime as dt
import sys, os

# ------------------------ User input-----------------------------------

# days = ["2017-05-23", "2017-05-24"] # for Neff
# days = ["2017-06-14", "2017-06-15", "2017-06-16"] # for Jaume Mata
#days = ["2017-03-30", "2017-03-31", "2017-04-03", "2017-04-04", "2017-04-05", "2017-04-06", "2017-04-07"] # for Peter Rindt
#days = ["2017-06-06", "2017-06-08"] # for Stan
#days = ["2017-01-26", "2017-01-26"] # for Marc
#days = ["2017-10-25"] # for Yuki
#days = ["2018-01-17"] # for JET / heatpipe
#days = ["2017-11-01"] # for Yuli
#days = ["2018-01-30", "2018-01-31"] # for Jun
#days = ["2018-02-21"] # highB
#days = ["2018-02-06", "2018-02-07", "2018-02-08", "2018-02-09", "2018-02-13", "2018-02-14", "2018-02-15", "2018-02-16"] # for Brandon
#days = ["2018-03-20", "2018-03-21"] # for Greg
#days = ["2018-03-15"]
#days = ["2018-03-15", "2018-03-16", "2018-03-19", "2018-03-20", "2018-03-21"]
#days = ["2018-02-20"]
#days = ["2018-08-27","2018-08-28","2018-08-29","2018-08-30","2018-08-31"]
#days = ["2018-09-18", "2018-09-19", "2018-09-20", "2018-09-21"]
#days = ["2018-10-03","2018-10-04","2018-10-05"] # Masayuki
#days = ["2018-12-11", "2018-12-12", "2018-12-14", "2018-12-18",] # Yu Li / Swip
#days = ["2014-10-24"] # Alexey Razdobarin
#days = ["2018-11-06","2018-11-07","2018-11-08","2018-11-09"] # Claudiu Costin
#days = ["2019-03-26","2019-03-27","2019-03-28","2019-03-29"] # Yu Li
#days = ["2019-04-18"] # Yu Li
#days = ["2019-04-17","2019-04-18","2019-04-19","2019-04-23"] # For Wei
#days = ["2019-05-07","2019-05-08","2019-05-09","2019-05-10"] # For Janina
#days = ["2019-06-03", "2019-06-06"] # Jack & Joe
#days = ["2019-07-30","2019-07-31","2019-08-01","2019-08-02"] # Jonathan
#days = ["2019-08-06","2019-08-07","2019-08-08","2019-08-09"] # ITER Thomas
#days = ["2019-08-27","2019-08-28"] # Wei
#days = ["2019-09-17","2019-09-18","2019-09-19","2019-09-20","2019-09-24","2019-09-25","2019-09-26"] # Yuki & Masa
#days = ["2019-11-26","2019-11-27","2019-11-28","2019-12-03","2019-12-04","2019-12-05"] # Hennie
#days = ["2020-01-17","2020-01-24"] # Wei
#days = ["2020-02-07"] # Wei
#days = ["2018-11-20","2018-11-21","2019-03-22","2019-09-18","2019-09-19","2020-02-18","2020-02-19"] # Shin
#days = ["2020-05-26","2020-05-27","2020-05-28","2020-05-29"] # YuLi
#days = ["2020-07-14","2020-07-15"] # Wei/Fabio
#days = ["2020-12-09"] # Peter Rindt
#days = ["2020-12-01","2020-12-02","2020-12-03"] # Claudiu Costin
#days = ["2021-01-26","2021-01-27","2021-01-28","2021-01-29"] # For SWIP / Yu Li
#days = ["2021-02-23","2021-02-24","2021-02-25","2021-02-26"] # Emre Yildirim
#days = ["2020-06-30"] # Stan Camp
#days = ["2021-03-02","2021-03-03","2021-03-04","2021-03-05"]
#days = ["2021-06-08"] # Rabel
days = ["2021-06-10"]
outDir = "output/thomson/"

# ------------------------ Functions -----------------------------------

def humanTimeStr(dbTimeStamp):
	return client.timetostring(dbTimeStamp & 0xffffffff00000000).split()[1]

def humanDateStr(dbTimeStamp):
	return client.timetostring(int(dbTimeStamp) & 0xffffffff00000000).split()[0]

def humanTimeMsStr(dbTimeStamp):
	# split into date, time
	# take time part
	timeString = client.timetostring(dbTimeStamp).split()[1]
	if ("." in timeString):
		# remove "000" at the end
		return timeString[:-3]
	else:
		# add ".000" at the end
		return timeString + ".000"

# ------------------------ Main program --------------------------------

if not os.path.exists(outDir):
    os.makedirs(outDir)

# Connect to database
db = client.connectToDbService(settings.store)

# Get variable handle (take only first item of 1 element list, index 0)
pTsSequenceNr = db.findNode("TsSequenceNr")[0]
pTsLaserEnergy = db.findNode("TsLaserEnergy")[0]
#pTsTrigExp = db.findNode("TsTrigExp")[0]

# Get variable handle (take only first item of 1 element list, index 0)
pTsProfNe = db.findNode("TsProfNe")[0]
pTsProfTe = db.findNode("TsProfTe")[0]
pTsProfNe_d = db.findNode("TsProfNe_d")[0]
pTsProfTe_d = db.findNode("TsProfTe_d")[0]
pTsRadCoords = db.findNode("TsRadCoords")[0]

TIMES = 0; DATA = 1
eV = 1.602e-19 #J/eV

trigFile = outDir + "Thomson_triggers.txt"
f2 = open(trigFile, 'w')
f2.write("Trigger number [#]" + "\t" + "Date [yyyy-mm-dd]" + "\t" + "Time [hh:mm:ss.ms]" + "\n")

# Loop days ...
for day in days:

	# Create time range
	startTime = client.datetotime(dt.datetime.strptime(day + " 00:08:00", client.DATE_FORMAT))
	endTime = client.datetotime(dt.datetime.strptime(day + " 20:00:00", client.DATE_FORMAT))
	timeRange = client.TimeRange(startTime, endTime)
	
	# Get triggers
	TsSequenceNr = pTsSequenceNr.read(timeRange, 0, 0, unit=client.SI_UNIT)
	numTriggers = len(TsSequenceNr[TIMES])
	print ("numTriggers: ", numTriggers)
	
	# loop trigger times
	for i, triggerTime in enumerate(TsSequenceNr[TIMES]):
		trigNum = (TsSequenceNr[DATA][i])
		print ("triggerTime: ", humanDateStr(triggerTime), humanTimeMsStr(triggerTime))
		timeStrForFileName = humanTimeMsStr(triggerTime).replace(":", "_")
		
		# Create time range - data
		if (i < numTriggers-1):
			nextTriggerTime = TsSequenceNr[TIMES][i+1]
		else:
			nextTriggerTime = endTime
		timeRangeData = client.TimeRange(triggerTime, nextTriggerTime-1)
				
		# get all measurements in data range 
		TsProfNe = pTsProfNe.read(timeRangeData, 0, 0, unit=client.SI_UNIT)
		TsProfTe = pTsProfTe.read(timeRangeData, 0, 0, unit=client.SI_UNIT) # BUG: PREF_UNIT crashes
		TsProfNe_d = pTsProfNe_d.read(timeRangeData, 0, 0, unit=client.SI_UNIT)
		TsProfTe_d = pTsProfTe_d.read(timeRangeData, 0, 0, unit=client.SI_UNIT) # BUG: PREF_UNIT crashes
		TsRadCoords = pTsRadCoords.read(timeRangeData, 0, 0, unit=client.SI_UNIT)
		
		numMeasurements = len(TsProfNe[TIMES])
		print ("numMeasurements: ", numMeasurements)
		
		# Skip trigger if no profiles found
		if (numMeasurements == 0):
			continue
		
		f2.write(str(trigNum) + "\t" + humanDateStr(triggerTime) + "\t" + humanTimeMsStr(triggerTime) + "\n")
		
		# create file per day, per trigger
		#outputFile = outDir + day + "-TsProfiles-" + timeStrForFileName + ".txt"
		outputFile = outDir + "Thomson_" + day + "_#" + str(trigNum) + ".txt"
		print ("Output:", outputFile)
		f = open(outputFile, 'w')
		
		# get extra data: TsLaserEnergy, TsTrigExp
		timeRangeExtraData = client.TimeRange(client.MIN_TIME, triggerTime)
		TsLaserEnergy = pTsLaserEnergy.getLast(timeRangeExtraData)[DATA]
		#TsTrigExp = pTsTrigExp.getLast(timeRangeExtraData)[DATA]
		print ("TsLaserEnergy: ", TsLaserEnergy)
		#print "TsTrigExp: ", TsTrigExp
		
		# write header
		f.write("Trigger Time:\t" + humanDateStr(triggerTime) + "\t" + humanTimeMsStr(triggerTime) + "\n")
		f.write("Number of processed measurements:\t" + str(numMeasurements) + "\n")
		f.write("TsLaserEnergy:\t" + str(TsLaserEnergy) + "\n")
		#f.write("TsTrigExp:\t" + str(TsTrigExp) + "\n")
		#f.write("\n")
		
		# loop measurements
		for i, timeValueData in enumerate(TsProfNe[TIMES]):
			f.write("\n")
			numData = len(TsProfNe[DATA][i])
			print (humanDateStr(timeValueData) , humanTimeMsStr(timeValueData), timeValueData, " numData: ", numData)
			f.write("Measurement time:\t" + humanDateStr(timeValueData) + "\t" + humanTimeMsStr(timeValueData) + "\n")
			f.write("Rad. Pos [mm]" + "\t" + "ne [m-3]" + "\t" + "Te [eV]" + "\t" + "dne [m-3]" + "\t" + "dTe [eV]" + "\n")
			
			# loop data points
			for j in range(numData):
				#print TsRadCoords[DATA][i][j], "\t",
				#print TsProfNe[DATA][i][j], "\t",
				#print TsProfTe[DATA][i][j] / eV, "\t", # 
				#print TsProfNe_d[DATA][i][j], "\t",
				#print TsProfTe_d[DATA][i][j] / eV
				f.write(str(TsRadCoords[DATA][i][j]) + "\t" + str(TsProfNe[DATA][i][j]) + "\t" + str(TsProfTe[DATA][i][j] / eV) + "\t" + str(TsProfNe_d[DATA][i][j]) + "\t" + str(TsProfTe_d[DATA][i][j] / eV)+ "\n")
			
		# end loop measurements
		f.close()
	# end loop trigger times
f2.close()
