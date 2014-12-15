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