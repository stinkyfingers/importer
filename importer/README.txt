FILES

**BaseVehiclesNeeded
SQL for vehicles from the csv in which all basevehicles have the same array of parts and need a curt-centric VehicleID with submodel and config ID's of 0.

**BaseVehicle_PartsNeeded
SQL for inserting vcdb_VehicleParts. These vehicles are baseVehicles. Uses "old" Aries part numbers.


**SubmodelNeeded
SQL for vehicles from the csv in which all submodels have the same array of parts and need a curt-centric VehicleID with config ID of 0.

**Submodel_PartsNeeded
SQL for inserting vcdb_VehicleParts. These vehicles are submodels. Uses "old" Aries part numbers.

**MissingConfigTypes
This csv lists the names of AAIA ConfigurationAttributeTypes for which there does not appear to be a Curt vehicle attribute type in the ConfigAttributeType table. There are repeats.

**MissingConfigs
This csv lists AAIA ConfigurationAttributes and AAIAConfigurationAttributeTypes for pairings which do not currently have Curt-centric Config and ConfigAttribute values in the database. There are repeats.

**VehicleConfigurationsNeeded
This csv lists ConfigAttributeID, ConfigAttributeTypeID, AAIABaseID, AAIASubmodelID for vehicles. These listings need vehicles with the specified attribute & attributeType inserted in the vcdb_Vehicle table and the VehicleConfigAttribute table. It is a list of vehicles that need to be added before Parts can be added. They are config-unique vehicles. Clear as pie?


RUN
It's easy to run from the Test. 
Note: this is set up to use "old part numbers" from aries (as strings). The application wants the Aries parts to be already in the Parts table prior to running. It works without, but won't find any part matches.

Run(file, 1, dbName); file="nameofcsv.csv", headerlines=int(number of lines to skip), dbName="what you'd like to name the mongoDb collection".

RunAfterMongo(db string); takes the name of the mongoDb collection you're using; the db itself is "importer". This function runs the diff and outputs files with SQL and CSV records (above).