FILES

**BaseVehiclesNeeded
SQL for vehicles from the csv in which all basevehicles have the same array of parts and need a curt-centric VehicleID with submodel and config ID's of 0.

**BaseVehicle_PartsNeeded
SQL for inserting vcdb_VehicleParts. These vehicles are baseVehicles. Uses "old" Aries part numbers.

**BaseVehiclesNeededInBaseVehicleTable
Lists AAIABaseVehicleIDs for which there is no Curt BaseVehicleID - see BaseVehicleInserts...

**BaseVehicleInserts - redundant with BasevehiclesNeeded
Insert Statements (BaseVehicle table) for previsously non-existant BaseVehicles

**UnknownBaseVehicle
These are tricky - we are missing the aaia model and/or aaia make in the vcbd_Make or vcdb_Model tables. May be best to create these vehicles by hand.


**SubmodelNeeded
SQL for vehicles from the csv in which all submodels have the same array of parts and need a curt-centric VehicleID with config ID of 0.

**Submodel_PartsNeeded
SQL for inserting vcdb_VehicleParts. These vehicles are submodels. Uses "old" Aries part numbers.

**SubmodelInserts
Insert Statements (Submodel table) for previsously non-existent Submodels

**MissingConfigTypes
This csv lists the names of AAIA ConfigurationAttributeTypes for which there does not appear to be a Curt vehicle attribute type in the ConfigAttributeType table. There are repeats.

**MissingConfigs
This csv lists AAIA ConfigurationAttributes and AAIAConfigurationAttributeTypes for pairings which do not currently have Curt-centric Config and ConfigAttribute values in the database. There are repeats.

**VehicleConfigurationsNeeded
This csv lists ConfigAttributeID, ConfigAttributeTypeID, AAIABaseID, AAIASubmodelID for vehicles. These listings need vehicles with the specified attribute & attributeType inserted in the vcdb_Vehicle table and the VehicleConfigAttribute table. It is a list of vehicles that need to be added before Parts can be added. They are config-unique vehicles. Clear as pie?

**PartsNeededInPartTable
This is a list of OldPartNumbers for which there is no "new" Curt PartNumber.


RUN

1) Run MongoDB
2) Import Aries Parts into the CurtDev Part table

It's easy to run from the Test. 
Note: this is set up to use "old part numbers" from aries (as strings). The application wants the Aries parts to be already in the Parts table prior to running. It works without, but won't find any part matches.

3) ImportCsv(file, 1, dbName); file="nameofcsv.csv", headerlines=int(number of lines to skip), dbName="what you'd like to name the mongoDb collection".

4) RunDiff(db string); takes the name of the mongoDb collection you're using; the db itself is "importer". This function runs the diff and outputs files with SQL and CSV records (above).
	a) see PartsNeededInPartTable.csv - it's probably best to enter missing parts before re-running the diff

5) Run GetQueriesForNewBaseVehiclesAndSubmodels 

6) Run GetQueriesToInsertConfigs

7) Run Queries from BaseVehicle_PartsNeeded & BaseVehicleInserts & Submodel_PartsNeeded &  SubmodelInserts & BaseVehicleInserts & MissingConfigInserts in your SQL client.



8) Repeat #4-8. As you add BaseVehicles (BaseVehicleInserts), more new queries in BaseVehicle_PartsNeeded will be generated. Same for SubmodelInserts & Submodel_PartsNeeded. Same for configs.



 