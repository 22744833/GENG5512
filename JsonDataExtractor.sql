--author: Josh Radich
--Student No: 22744833
--Unit: GENG5512
--Desc: This script shows how to extract data from a json or geojson file and prepare it for importing into an sql database

declare @geoJson2 varchar(max)
select @geoJson2 = BulkColumn
from openrowset 
(bulk 'C:\Uni\GENG5511\railsmart-data-prod\Geojson\perth_sa1.json',single_clob)  --specify json file 
as j

select 
	
	min(SA1_MAIN16) as SA1_MAIN16,-- name columns of spatial meta data
	min(GCC_NAME16) as GCC_NAME16,--
	min(SA2_NAME16) as SA2_NAME16,--
	min(AREASQKM16) as AREASQKM16,--
	min(GCC_CODE16) as GCC_CODE16,--
	min(STE_CODE16) as STE_CODE16,--
	min(SA2_MAIN16) as SA2_MAIN16,--
	min(SA1_7DIG16) as SA1_7DIG16,--
	min(SA4_CODE16) as SA4_CODE16,--
	min(STE_NAME16) as STE_NAME16,--
	min(SA3_NAME16) as SA3_NAME16,--
	min(SA4_NAME16) as SA4_NAME16,--
	min(SA3_CODE16) as SA3_CODE16,--
	min(SA2_5DIG16) as SA2_5DIG16,--
	STRING_AGG(CAST(Long as nvarchar(max)) + ' ' + cast(Lat as nvarchar(max)), ',') as COORDARG --aggregate the coordinate array into single row 
	from 
	openjson(@geoJson2,'$.features')
	with(
		SA1_MAIN16 varchar(100) '$.properties.SA1_MAIN16',-- extract json from json file 
		GCC_NAME16 varchar(100) '$.properties.GCC_NAME16',--
		SA2_NAME16 varchar(100) '$.properties.SA2_NAME16',--
		AREASQKM16 varchar(100) '$.properties.AREASQKM16',--
		GCC_CODE16 varchar(100) '$.properties.GCC_CODE16',--
		STE_CODE16 varchar(100) '$.properties.STE_CODE16',--
		SA2_MAIN16 varchar(100) '$.properties.SA2_MAIN16',--
		SA1_7DIG16 varchar(100) '$.properties.SA1_7DIG16',--
		SA4_CODE16 varchar(100) '$.properties.SA4_CODE16',--
		STE_NAME16 varchar(100) '$.properties.STE_NAME16',--
		SA3_NAME16 varchar(100) '$.properties.SA3_NAME16',--
		SA4_NAME16 varchar(100) '$.properties.SA4_NAME16',--
		SA3_CODE16 varchar(100) '$.properties.SA3_CODE16',--
		SA2_5DIG16 varchar(100) '$.properties.SA2_5DIG16',--
		COORDS nvarchar(max) '$.geometry.coordinates[0]' as json 
	)as coords
	cross apply openjson(coords) with --cross apply in order to properly have access to all coordinates in array 
	(
		lat varchar(100) '$[0]',
		long varchar(100) '$[1]'
	)
	group by COORDS


