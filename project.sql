CREATE TABLE air_quality(uniqueid int, indicatorid int, name String, measure String, measureinfo String, geotypename String, geoplacename String, timeperiod String, startdate String, datavalue float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DESCRIBE air_quality;

LOAD DATA LOCAL INPATH '/home/tpm308/FinalProject/CleanedAirQualityData.csv'
OVERWRITE INTO TABLE air_quality;

SELECT * FROM air_quality LIMIT 10;

CREATE TABLE water_quality(sampledate String, samplesite String, location String, residualfreechlorine float, turbidity float, fluoride float, coliform String, ecoli String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DESCRIBE water_quality;

LOAD DATA LOCAL INPATH '/home/tpm308/FinalProject/CleanDateWaterData.csv'
OVERWRITE INTO TABLE water_quality;

SELECT * FROM water_quality LIMIT 10;

