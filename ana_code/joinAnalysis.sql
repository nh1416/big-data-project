USE tpm308;
--CREATE TABLE air_quality(uniqueid int, indicatorid int, name String, measure String, measureinfo String, geotypename String, geoplacename String, timeperiod String, startdate String, datavalue float)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY ','
--STORED AS TEXTFILE;

DESCRIBE air_quality;

--LOAD DATA LOCAL INPATH '/home/tpm308/FinalProject/CleanedAirQualityData.csv'
--OVERWRITE INTO TABLE air_quality;

SELECT * FROM air_quality LIMIT 10;

--CREATE TABLE water_quality(sampledate String, samplesite String, location String, residualfreechlorine float, turbidity float, fluoride float, coliform String, ecoli String)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY ','
--STORED AS TEXTFILE;

DESCRIBE water_quality;

--LOAD DATA LOCAL INPATH '/home/tpm308/FinalProject/CleanDateWaterData.csv'
--OVERWRITE INTO TABLE water_quality;

SELECT * FROM water_quality LIMIT 10;

--Query to show all of the relevant data in both sets together
SELECT air_quality.startdate, avg(air_quality.datavalue), avg(water_quality.residualfreechlorine), avg(water_quality.turbidity), avg(water_quality.fluoride) 
FROM air_quality 
FULL JOIN water_quality 
ON air_quality.startdate = water_quality.sampledate 
GROUP BY air_quality.startdate
ORDER BY SUBSTR(air_quality.startdate, 4), SUBSTR(air_quality.startdate, 0, 2);
