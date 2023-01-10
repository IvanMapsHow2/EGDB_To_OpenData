#PURPOSE
#   Reloads given open-data AGO feature-layers (or hosted tables) w/ data from source EGDB (enterprise geodatabase) feature-classes or tables.
#   Reloads by truncating then appending. Designed to be run as an automated task.

#TERMINOLOGY USED IN COMMENTS:
#   EGDB source: A feature class or non-spatial table in an EGDB.
#
#   feature service: AGO feature service.
#
#   feature layer: AGO hosted feature-layer or hosted table that lives in a feature service.

#HOW IT WORKS (PSEUDO CODE)
#   Set major variables, including pointers to EGDB sources and feature-layer counterparts.
#   Make a GUID-based temporary subfolder in script's folder for staging data to be loaded.
#   Get day of week (U,M,T,W,R,F,S).
#   For each of given EGDB sources and feature-layer counterparts:
#      If current day is a day on which that EGDB source/feature layer is worked:
#         Make a temporary .gdb in temporary subfolder; name the .gdb:
#            DeleteMe_<Item ID of feature service>.gdb.
#         Copy EGDB source from EGDB into temporary .gdb.
#         Capture pre-append record count of feature class or non-spatial table.
#         Zip the .gdb into a .zip in the temporary subfolder; named the .zip:
#            DeleteMe_<Item ID of feature service>.gdb.zip.
#         Upload the .zip to AGO.
#         Capture pre-append record count of feature layer.
#         Truncate the feature layer.
#         Append to the feature layer from the uploaded .zip.
#         Capture post-append record count of feature layer.
#         Delete the uploaded .zip (from AGO).
#   Delete GUID-based temporary subfolder.
#   Email a report.

#README NOTES
#   -This script writes its activity into a log file named EGDB_To_OpenData.log
#    in the script's folder.
#
#   -This script is designed to work w/ feature services that have ONLY 1 feature layer (or hosted table). When getting the
#    feature layer to truncate and append, it gets the first (should be only) feature layer (or hosted table) of the service.
#
#    For example, if given a feature-service URL that ends w/ FS_VCGI_OPENDATA_Emergency_ESITE_point_SP_v1/FeatureServer,
#    the script assumes that the first layer (e.g., FS_VCGI_OPENDATA_Emergency_ESITE_point_SP_v1/FeatureServer/0) of that
#    service is the only feature layer of that service.
#
#   -If EGDB source's schema is changed and different from the feature-layer counterpart, modify the
#    feature layer's schema to match the EGDB source before running this script.
#
#   -Sometimes an upload task or a delete+append task fails for no apparent reason (maybe due to break in connection?).
#    Because of this, this script loops through those tasks up to a given maximum number of tries.

#HISTORY
#   Written by Ivan Brown on 2021-09-03, using:
#      Python 3.7.10, ArcGIS Pro 2.8.1, and ArcGIS Python API,
#
#   and referencing script from Esri Community that demonstrates truncate+append approach:
#      https://community.esri.com/t5/arcgis-online-documents/overwrite-arcgis-online-feature-service-using/ta-p/904457
#
#   Modified by Ivan Brown on 2021-09-19 to include loops that re-try uploads and truncate+appends up to a maximum
#   number of times if they fail.

#HOW TO USE
#   1) Set major variables in section of this script commented as ***** SET MAJOR VARIABLES HERE *****.
#
#   2) Run or schedule to run. Run on a machine that has ArcGIS Pro.
#      Run using this command: <program files>\ArcGIS\Pro\bin\Python\scripts\propy.bat <this script file>

#******************** SET MAJOR VARIABLES HERE ***********

#layers
#   Set "layers" to a list of lists, w/ each list having this sequence (1 list for each EGDB source/feature layer):
#      0: Name of .sde file that get's EGDB source. The .sde file must be in the script's folder.
#         Save login to .sde file if this script runs as an automated task.
#
#      1: If EGDB source is in a feature-dataset, name of feature dataset (include schema prefix). Otherwise, set to empty string.
#
#      2: EGDB-source (feature class or non-spatial table) name (include schema prefix).
#
#      3: Title of feature service that contains feature layer (must be a non-case-sensitive match for script to work).
#
#      4: Item ID of feature service that contains the feature layer.
#
#      5: Day(s) of week (separated by commas) on which feature layer is reloaded. Valid days (represented by single characters) are:
#            "U" -Sunday
#            "M" -Monday
#            "T" -Tuesday
#            "W" -Wednesday
#            "R" -Thursday
#            "F" -Friday
#            "S" -Saturday
#
#   For example:
#      layers = []
#      layers.append(["BigCity.sde", "", "BigCity.GISadmin.parcels", "Big City Parcels", "77f362f2-9efd-4420-a7fc-6560fd83ef6d", "T"])
#      layers.append(["BigCity.sde", "", "BigCity.GISadmin.streets", "Big City Streets", "3169e5af-e0b4-4685-bb93-f2605f9391cb", "W"])
#(Using implicit line joins--defining items over multiple lines--to make the list more readable).
layers = []
layers.append(["BigCity.sde",
               "",
               "BigCity.GISadmin.parcels",
               "Big City Parcels",
               "ac32ee49-9648-4963-8e61-b974372852e7",
               "U"])

#AGO u and p.
#   IMPORTANT!: This script uploads source data to AGO as a temporary file geodatabase, uses that temporary file geodatabase to reload
#               feature layers, and then deletes the temporary file geodatabase from AGO. Make sure that you use the
#               appropriate AGO user. The temporary file geodatabase is uploaded to that user's content. The feature service being
#               reloaded needs to be owned by that user.
u = ""
p = ""

#Set content_folder to folder (folder name) in AGO user's content that is designated for containing temporary file geodatabases.
content_folder = "temp"

#Set max_tries to an integer to indicate the maximum number of times script should try each of these 2 tasks:
#      -upload source data to AGO.
#
#      -truncate-and-append to reload data.
#
#   Sometimes something goes wrong during these tasks (maybe a break in connection?). These tasks are tried up to max_tries times
#   before causing the script to terminate.
max_tries = 3

#email_server
#   The host name of the SMTP router to be used for sending email report.
email_server = ""
#
#email_port
#   The port number of the SMTP router to be used for sending email. Set to a string.
email_port = ""
#
#email_from
#   The sender email address to be used with email notifications (must be in
#   name@domain format). An email account that is used for automated notifications in
#   your organization can be used.
email_from = ""
#
#to_list
#   This setting is used to store email addresses of email recipients (must be in
#   name@domain format). Set to a Python list.
#
#   For example:
#
#      ["name1@domain1","name2@domain2"]
#
to_list = []
to_list.append("name@domain")
#******************** END SECTION FOR MAJOR VARIABLES ****

#MODULES
print("Importing modules...")
import os
import sys
import arcpy
import time
import smtplib
import zipfile
import uuid
from arcgis.gis import GIS

#GLOBAL VARIABLES

#email_content collects info to go into email notifications
email_content = ""

#FUNCTIONS

#THIS FUNCTION SIMPLY CAPTURES THE CURRENT DATE AND TIME AND
#   RETURNS IN A PRESENTABLE TEXT FORMAT YYYYMMDD-HHMM
#   FOR EXAMPLE:
#      20171201-1433
def tell_the_time():
   the_year = str(time.localtime().tm_year)
   the_month = str(time.localtime().tm_mon)
   the_day = str(time.localtime().tm_mday)
   the_hour = str(time.localtime().tm_hour)
   the_minute = str(time.localtime().tm_min)
   #FORMAT THE MONTH TO HAVE 2 CHARACTERS
   while len(the_month) < 2:
      the_month = "0" + the_month
   #FORMAT THE DAY TO HAVE 2 CHARACTERS
   while len(the_day) < 2:
      the_day = "0" + the_day
   #FORMAT THE HOUR TO HAVE 2 CHARACTERS
   while len(the_hour) < 2:
      the_hour = "0" + the_hour
   #FORMAT THE MINUTE TO HAVE 2 CHARACTERS
   while len(the_minute) < 2:
      the_minute = "0" + the_minute
   the_output = the_year + the_month + the_day + "-" + the_hour + the_minute
   return the_output

#THIS FUNCTION SIMPLY TAKES A STRING ARGUMENT AND THEN
#   WRITES THE GIVEN STRING INTO THE SCRIPT'S LOG FILE (AND
#   OPTIONALLY PRINTS AND/OR EMAILS IT).
#   SET FIRST ARGUMENT TO THE STRING. SET THE SECOND
#   ARGUMENT (BOOLEAN) TO True OR False TO INDICATE IF
#   STRING SHOULD ALSO BE PRINTED. SET THE THIRD
#   ARGUMENT (BOOLEAN) TO True OR False TO INDICATE IF
#   STRING SHOULD ALSO BE INCLUDED IN EMAIL NOTIFICATION.
#   ADDS CURRENT TIME TO BEGINNING OF FIRST PARAMETER.
#   ADDS A \n\n TO FIRST PARAMETER (FOR HARD RETURNS).
def make_note(the_note, print_it = False, email_it = False):
   the_note = tell_the_time() + "  " + the_note
   the_note += "\n\n"
   log_file = open(sys.path[0] + "\\EGDB_To_OpenData.log", "a")
   log_file.write(the_note)
   log_file.close()
   if print_it == True:
      print(the_note)
   if email_it == True:
      global email_content
      email_content += the_note

#THIS FUNCTION SENDS A GIVEN MESSAGE TO AN EMAIL DISTRIBUTION-LIST.
#   THE FIRST ARGUMENT IS THE EMAIL'S SUBJECT STRING.
#   THE SECOND ARGUMENT IS THE EMAIL'S MESSAGE-CONTENT STRING.
def send_email(the_subject = "", the_message = ""):
   the_header = 'From:  "Python" <' + email_from + '>\n'
   the_header += "To:  Open-Data Watchers\n"
   the_header += "Subject:  " + the_subject + "\n"
   #INSTANTIATE AN SMTP OBJECT
   smtp_serv = smtplib.SMTP(email_server + ":" + email_port)
   #SEND THE EMAIL
   smtp_serv.sendmail(email_from, to_list, the_header + the_message)
   #QUIT THE SERVER CONNECTION
   smtp_serv.quit()

#THIS FUNCTION RETURNS DATA-OBJECT NAME (MINUS SCHEMA PREFIX, (DATABASE.OWNER.)) FROM A GIVEN DATA-OBJECT NAME (FEATURE CLASS, TABLE, OR RASTER DATASET).
#   IF THE DATA OBJECT HAS NO SCHEMA PREFIX (E.G., FILE-GEODATABASE FEATURE-CLASS), RETURNS THE GIVEN NAME.
def get_name(the_data_object):
   i = the_data_object.rfind(".")
   if i == -1:
      return the_data_object
   else:
      return the_data_object[i + 1:len(the_data_object)]

#THIS FUNCTION TAKES A FEATURE-CLASS OR NON-SPATIAL TABLE AND RETURNS ITS ROW COUNT (AS STRING).
def get_count(the_data_object):
   return arcpy.GetCount_management(the_data_object)[0]

try:
   #MAKE SURE GIVEN EGDB-SOURCES EXIST
   make_note("Making sure given EGDB sources exist...", True)
   for i in layers:
      j = os.path.join(sys.path[0], i[0])
      if i[1] != "":
         j = os.path.join(j, i[1])
      j = os.path.join(j, i[2])
      if arcpy.Exists(j) == False:
         make_note("Couldn't find EGDB-source " + i[2] + " (source for feature-layer " + i[3] + "). Script terminated.", True, True)
         sys.exit()
      #(APPEND FULL PATH AS NEW ITEM ON LIST)
      i.append(j)
         
   #CONNECT TO ARCGIS ONLINE
   make_note("Connecting to AGO...", True)   
   gis = GIS("https://www.arcgis.com",  username = u, password = p)

   #MAKE SURE GIVEN FEATURE LAYERS EXIST AND ARE IN 1-LAYER FEATURE SERVICES
   make_note("Making sure given feature layers exist and are in 1-layer feature services...", True)
   for i in layers:
      j = gis.content.get(i[4])
      if j == None:
         make_note("Couldn't find item w/ ID " + i[4] + " (given title is + " + i[3] + "). Script terminated.", True, True)
         sys.exit()
      if j.title.lower() != i[3].lower():
         make_note("Item w/ ID " + i[4] + " wasn't matched to an item w/ given title " + i[3] + ". Title w/ that ID is " + j.title + ". Script terminated.", True, True)
         sys.exit()
      if len(j.layers) != 1:
         #(IF LENGTH OF layers ISN'T 1, SEE IF LENGTH OF tables IS 1, WHICH WOULD BE THE CASE IF FEATURE SERVICE HOSTS A NON-SPATIAL TABLE)
         if len(j.tables) != 1:
            make_note("Item w/ ID " + i[4] + " (given title is " + i[3] + ") isn't a 1-layer feature-service as expected. Script terminated.", True, True)
            sys.exit()      
   
   #CREATE A TEMPORARY GUID-NAMED SUBFOLDER IN SCRIPT'S FOLDER TO ASSEMBLE EGDB-SOURCE DATA FOR UPLOAD TO AGO
   make_note("Creating temporary subfolder to assemble EGDB-source data to be loaded...", True)
   the_GUID = str(uuid.uuid4())
   os.mkdir(os.path.join(sys.path[0], the_GUID))
   temp_subfolder = os.path.join(sys.path[0], the_GUID)
   make_note("Temporary subfolder for assembly of EGDB-source data is " + the_GUID + ".", True)

   #GET CURRENT DAY OF WEEK
   s = time.localtime()
   the_day = s.tm_wday
   if the_day == 0:
      the_day = "M"
   elif the_day == 1:
      the_day = "T"
   elif the_day == 2:
      the_day = "W"
   elif the_day == 3:
      the_day = "R"
   elif the_day == 4:
      the_day = "F"
   elif the_day == 5:
      the_day = "S"
   else:
      the_day = "U"
   make_note("Today is day " + the_day + ".", True)

   #RELOAD EACH FEATURE LAYER
   make_note("Entering loop to reload each feature layer...", True)
   for i in layers:
      #FIND OUT IF FEATURE SERVICE GETS RELOADED TODAY
      okay_days = i[5].split(",")
      j = 0
      while j < len(okay_days):
         okay_days[j] = okay_days[j].upper().strip()
         j += 1
      #IF IT DOES, THEN RELOAD IT
      if the_day.upper() in okay_days:
         make_note("Feature service w/ title " + i[3] + " and ID " + i[4] + " gets reloaded today. Starting reload steps.", True, True)
         #MAKE FILE GEODATABASE
         #(NAME IT BASED ON ITEM ID OF RELATED FEATURE-SERVICE)
         gdb_name = "DeleteMe_" + i[4] + ".gdb"
         make_note("Making temporary geodatabase " + gdb_name + " ...", True)
         arcpy.management.CreateFileGDB(temp_subfolder, gdb_name)
         #COPY EGDB SOURCE INTO TEMPORARY FILE GEODTABASE
         #(IF IT'S A FEATURE CLASS)
         if arcpy.Describe(i[len(i) - 1]).dataType == "FeatureClass":
            make_note("Copying EGDB source (a feature class) into temporary file geodatabase " + gdb_name + " ...", True)
            arcpy.conversion.FeatureClassToFeatureClass(i[len(i) - 1], os.path.join(temp_subfolder, gdb_name), get_name(i[2]))
         #(OTHERWISE, IT MUST BE A NON-SPATIAL TABLE)
         else:
            make_note("Copying EGDB source (a non-spatial table) into temporary file geodatabase " + gdb_name + "...", True)
            arcpy.conversion.TableToTable(i[len(i) - 1], os.path.join(temp_subfolder, gdb_name), get_name(i[2]))
         #UPLOAD ZIP TO AGO####################
         success = False
         counter = 0
         gdb_properties={'title':gdb_name, 'type':'File Geodatabase', 'overwrite':True, 'description':'A temporary file for reloading data of feature service ' + i[3] + ', which has Item-ID ' + i[4] + '. This file can be deleted after reload.'}
         while success == False and counter < max_tries:
            try:
               #ZIP THE FILE GEODATABASE
               make_note("Zipping temporary file geodatabase...", True)
               #(GIVING THE FILE GEODATABASE IN THE ZIP A DIFFERENT GUID-BASED NAME TO MAKE SURE NAME IS UNIQUE IN AGO)
               gdb_name_for_uploading = "DeleteMe_" + str(uuid.uuid4()) + ".gdb"
               zip_path = os.path.join(temp_subfolder, gdb_name_for_uploading + ".zip")
               the_zip = zipfile.ZipFile(zip_path, 'x')
               gdb_files = os.listdir(os.path.join(temp_subfolder, gdb_name))
               for a_file in gdb_files:
                  the_zip.write(os.path.join(temp_subfolder, gdb_name, a_file), os.path.join(gdb_name_for_uploading, a_file))
               the_zip.close()
               make_note("Making fresh connection to AGO...", True)
               del gis
               gis = GIS("https://www.arcgis.com",  username = u, password = p)
               make_note("Try #" + str(counter + 1) + " - Uploading zipped temporary geodatabase " + gdb_name_for_uploading + ".zip to AGO...", True)
               gdb_item = gis.content.add(item_properties=gdb_properties, data=zip_path, folder=content_folder)
               #(CAPTURE ITEM ID OF THE UPLOADED GEODATABASE)
               gdb_item_id = gdb_item.id
               success = True
            except:
               make_note("Something went wrong w/ uploading.", True)
            counter += 1
         if success == False:
            make_note("A problem occurred when uploading zipped EGDB-source data to AGO for feature-service " + i[3] + "--tried " + str(max_tries) + " times. Script terminated. Clean up temporary .gdb's from folder " + content_folder + " in AGO (if there).", True, True)
            sys.exit()
         if counter > 1:
            make_note("ALERT - It took " + str(counter) + " tries to successfully upload zipped temporary geodatabase to AGO for feature-service " + i[3] + ". This likely left some temporary .gdb's in folder " + content_folder + " in AGO. Check folder for cleanup.", True, True)
         ####################
         #CAPTURE PRE-APPEND RECORD-COUNT OF EGDB SOURCE
         make_note("Record count of EGDB source " + get_name(i[2]) + " is " + get_count(i[len(i) - 1]) + ".", True, True)
         #DETERMINE IF FEATURE SERVICE HAS A SPATIAL LAYER OR A NON-SPATIAL LAYER (NON-SPATIAL TABLE)
         if len(gis.content.get(i[4]).layers) == 1:
            is_spatial = True
         else:
            is_spatial = False
         #CAPTURE PRE-APPEND RECORD-COUNT OF FEATURE LAYER
         if is_spatial == True:
            pre_append_feature_layer_count = str(gis.content.get(i[4]).layers[0].query(return_count_only = True))
         else:
            pre_append_feature_layer_count = str(gis.content.get(i[4]).tables[0].query(return_count_only = True))
         make_note("Before reloading, record count of feature layer in feature-service " + i[3] + " is " + pre_append_feature_layer_count + ".", True, True)
         #TRUNCATE+APPEND FEATURE LAYER####################
         success = False
         counter = 0
         while success == False and counter < max_tries:
            try:
               make_note("Making fresh connection to AGO...", True)
               del gis
               gis = GIS("https://www.arcgis.com",  username = u, password = p)
               make_note("Try #" + str(counter + 1) + " -  Truncating+appending feature-layer of feature-service " + i[3] + " ...", True, True)
               if is_spatial == True:
                  f_layer = gis.content.get(i[4]).layers[0]
               else:
                  f_layer = gis.content.get(i[4]).tables[0]
               #TRUNCATE
               make_note("First, truncating...", True)
               the_result = f_layer.manager.truncate()
               if str(the_result) != "{'success': True}":
                  make_note("Something went wrong with truncation.", True)
               else:
                  #APPEND
                  make_note("Appending...", True)
                  the_result = f_layer.append(item_id = gdb_item_id, upload_format = "filegdb", source_table_name = get_name(i[2]), upsert=False)
                  if the_result != True:
                     make_note("Something went wrong with the append.", True)
                  else:
                     success = True
            except:
               make_note("Something went wrong with truncating+appending.", True)
            counter += 1
         if success == False:
            make_note("A problem occurred when truncating+appending feature-service " + i[3] + "--tried " + str(max_tries) + " times. Script terminated.", True, True)
            sys.exit()
         else:
            make_note("Successful truncate+append.", True, True)
         ####################
         #CAPTURE POST-APPEND RECORD-COUNT OF FEATURE LAYER
         if is_spatial == True:
            post_append_feature_layer_count = str(gis.content.get(i[4]).layers[0].query(return_count_only = True))
         else:
            post_append_feature_layer_count = str(gis.content.get(i[4]).tables[0].query(return_count_only = True))
         make_note("After reloading, record count of feature layer in feature-service " + i[3] + " is " + post_append_feature_layer_count + ".", True, True)
         #DELETE TEMPORARY FILE GEODATABASE FROM AGO
         make_note("Deleting temporary file geodatabase " + gdb_name + " from AGO...", True)
         gdb_item = gis.content.get(gdb_item_id)
         the_result = gdb_item.delete()
         if the_result != True:
            make_note("ALERT - A problem occurred w/ deleting temporary file geodatabase " + gdb_name + " from AGO. This isn't a show stopper; however, it should be cleaned up.", True, True)
      else:
         pass
         
   #DELETE TEMPORARY SUBFOLDER
   make_note("Deleting temporary subfolder " + the_GUID + "...", True)
   arcpy.management.Delete(temp_subfolder)

   #EMAIL REPORT
   make_note("Emailing report...", True)
   send_email("EGDB_To_OpenData.py - REPORT", email_content)

   make_note("-----SCRIPT COMPLETED.", True)

except:
   make_note("-----Script terminated due to error condition. Check script's folder for temporary GUID-named subfolders that should be deleted (after possibly using them for troubleshooting).", True, True)
   #EMAIL REPORT
   send_email("EGDB_To_OpenData.py - ERROR", email_content)
