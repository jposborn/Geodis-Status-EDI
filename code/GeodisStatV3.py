import sys
import os
import glob
import logging
import configparser
import shutil
import xml.etree.cElementTree as et
from ftplib import FTP, all_errors
import smtplib
from email.mime.text import MIMEText
import csv
import pandas as pd
from datetime import date, timedelta


def readsgeodisifsumcsv(ifcsumcsv):
    os.chdir(programfolder)
    cols_to_use = [19, 83, 81, 82, 0]
    ifcsumcsv_df = pd.read_csv(ifcsumcsv, header=None, usecols=cols_to_use)[cols_to_use]
    ifcsumcsv_df.columns = ['GeoRef', 'GeoRef2', 'ConnNum', 'Address', 'Date']
    print(ifcsumcsv_df)
    ifcsumcsv_df = ifcsumcsv_df.drop_duplicates(subset=['GeoRef'], keep='last').reset_index(drop=True)
    ifcsumcsv_df.to_csv('IFCSUMData.csv', mode='a', header=False, index=False)

    ifcsumdata_df = pd.read_csv('IFCSUMData.csv')
    ifcsumdata_df = ifcsumdata_df.drop_duplicates(subset=['GeoRef'], keep='last').reset_index(drop=True)
    twoweeksago = date.today() - timedelta(14)
    ifcsumdata_df['Date'] = pd.to_datetime(ifcsumdata_df['Date'])
    ifcsumdata_df = ifcsumdata_df[ifcsumdata_df['Date'].dt.date >= twoweeksago]
    ifcsumdata_df.to_csv('IFCSUMData.csv', mode='w', index=False)
    return


def getifcsumdata(georef, filename):
    with open(filename, "r") as infile:
        reader = csv.reader(infile)
        next(reader)
        for line in reader:
            print(line[:1])
            if [georef] == line[:1]:
                return line


def readfclstatuscode(xmlfile):
    # todo Check if delivery code is DEL if True return Laser Reference, date, time, and message id, statdate, stattime
    #   call FCL to get address, importers ref, foreign agents ref
    try:
        tree = et.parse(programfolder + xmlfile)
        root = tree.getroot()
        for element in root.findall('.//Status'):
            statuscode = element.find('Event_Code').text
            if statuscode == 'DEL':
                sts = '21+310'
            elif statuscode == 'RET':
                sts = '23+20'
            elif statuscode == 'FLD':
                sts = '23+236'
            elif statuscode == 'DAM':
                sts = '23+218'
            elif statuscode == 'DEP' or statuscode == 'DEH' or statuscode == 'DEK' or statuscode == 'DEW':
                sts = '113+31'

            laserref = element.find('Reference_Number_1').text
            importersref = element.find('Reference_Number_2').text
            statusdate = element.find('Event_Date').text.replace('-', '')
            statustime = element.find('Event_Time').text
            statusdatetime = statusdate + statustime
            event_comments = element.find('Event_Comments').text
            print(laserref)

        for element in root.iter('Message_Header'):
            sendersunique = element.find('Senders_Unique').text
            messagedate = element.find('Date_of_Message_Creation').text.replace('-', '')
            messagetime = element.find('Time_of_Message_Creation').text
            messagedatetime = messagedate + messagetime

        ifcsumdata = getifcsumdata(importersref, ifcsumfile)

        georef2 = ifcsumdata[1]
        connum = ifcsumdata[2]
        address = ifcsumdata[3]

        dataset = [laserref, sendersunique, importersref, statusdatetime, messagedate,
                messagetime, messagedatetime, georef2, connum, address, sts, event_comments]
        return dataset

    except TypeError:
        errortype = "Convertion Error: FCL Status file " + xmlfile \
                    + " failed to convert to EDIFACT. IFCSUM Data is missing"
        logging.error(errortype)
        sendemail(errortype)
        shutil.move(programfolder + xmlfile, errorfolder)
        return ['ERROR']


def buildiftsta(data):
    os.chdir(programfolder)
    f = open((data[1] + ".edi"), "w")
    f.write('UNB+UNOC:4+CURRIE:ZZ+GEODIS-LOGISTICS:ZZZ+' + data[4][2:] + ':' + data[5] + '+' + data[1] + '\'\n')
    f.write('UNH+' + data[1] + '+IFTSTA:D:01B:UN\'\n')
    f.write('BGM+77+' + data[2] + '+9\'\n')
    f.write('DTM+137:' + data[6] + '00:204\'\n')
    f.write('NAD+MR+GNLENS::ZZZ++' + data[9] + '\'\n')
    f.write('NAD+MS+CURRIE::ZZZ++CURRIE\'\n')
    if data[10] == '21+310' and data[11] != '':
        f.write('NAD+AP+' + data[11] + '\'\n')
    f.write('CNI+1+' + data[8] + '\'\n')
    f.write('STS+1+' + data[10] + '\'\n')
    f.write('RFF+ACL:' + data[2] + '\'\n')
    f.write('RFF+ACD:' + data[7] + '\'\n')
    f.write('DTM+7:' + data[3] + ':203\'\n')
    f.write('UNT+11+' + data[1] + '\'\n')
    f.write('UNZ+1+' + data[1] + '\'\n')
    f.close()
    return


def ftpsendiftsta(filename, location):
    try:
        ftp = FTP(ceftpserver)
        ftp.login(ceftpuser, ceftppassword)
        ftp.cwd(ceftpfolder)
        with open((location + filename), 'rb') as p:
            ftp.storbinary('STOR ' + filename, p)
        logging.info("Geodis IFTSTA file " + filename + " uploaded to Currie AS2 Server")
        ftp.quit()
        shutil.move(location + filename, archivefolder)
        return
    except all_errors as e:
        shutil.move(location + filename, errorfolder)
        errortype = "FTP Error: Geodis ITFSTA file " + filename + " failed FTP upload to Currie AS2 Server - " + str(e)
        logging.error(errortype)
        sendemail(errortype)



def sendemail(error):

    msge = "The outbound Geodis EDI failed with the following error: \r\n" \
                                            "\r\n" \
                                            "######## " + error + " ########\r\n" \
                                            "\r\n" \
                                            "Please contact the IT department to investigate further" \
                                            " and re-transmit"

    msg = MIMEText(msge)

    msg['Subject'] = "Geodis EDI Error"
    msg['From'] = smtpsender
    msg['To'] = smtpreceiver

    s = smtplib.SMTP(smtpserver)
    s.send_message(msg)
    s.quit()
    return


def main():
    os.chdir(ftpinfolder)
    ifcsumlist = (glob.glob("*.csv"))

    if not ifcsumlist:
        logging.info("No Geodis EDIFACT Order File available")

    for file in ifcsumlist:
        print('***************************')
        print(file)
        shutil.move(ftpinfolder + file, programfolder + file)
        readsgeodisifsumcsv(file)

    for f in glob.glob(programfolder + '*_GEOBUS_CURRIE_GBOBX-IFCSUM_*.csv'):
        print(f)
        shutil.move(f, archivefolder)

    os.chdir(ftpinfolder)
    filelist = (glob.glob("*.xml"))

    if not filelist:
        logging.info("No Geodis Status Updates available")
        sys.exit()

    for file in filelist:
        print('---------------------------')
        print(file)
        shutil.move(ftpinfolder + file, programfolder + file)
        geodisdata = readfclstatuscode(file)
        if geodisdata[0] != 'ERROR':
            print(geodisdata)
            buildiftsta(geodisdata)
            shutil.move(programfolder + file, archivefolder)

    edifilelist = (glob.glob("*.edi"))
    for edifile in edifilelist:
        print('Sending EDI File.... ' + edifile)
        ftpsendiftsta(edifile, programfolder)


config = configparser.ConfigParser()
config.read('GeodisStatus.ini')
archivefolder = config['FOLDERS']['archive']
programfolder = config['FOLDERS']['program']
errorfolder = config['FOLDERS']['error']
ftpinfolder = config['FOLDERS']['ftpin']
fclhost = config['FCL DB']['host']
fcluser = config['FCL DB']['user']
fclpassword = config['FCL DB']['password']
fcldb = config['FCL DB']['db']
ceftpserver = config['FTP']['ceserver']
ceftpuser = config['FTP']['ceuser']
ceftppassword = config['FTP']['cepassword']
ceftpfolder = config['FTP']['cefolder']
smtpserver = config['EMAIL']['server']
smtpsender = config['EMAIL']['sender']
smtpreceiver = config['EMAIL']['receiver']
ifcsumfile = config['FILES']['ifcsumfilename']
nadmr = config['DATA']['nadmr']
loglevel = config['LOGGING']['level']

logging.basicConfig(
        filename='GeodisStatus.log', format='%(asctime)s:%(levelname)s:%(message)s', level=logging.getLevelName(loglevel))
main()
