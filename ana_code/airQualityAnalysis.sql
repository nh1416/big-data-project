USE tpm308;

DESCRIBE air_quality;

--query to determine if there has been a particular trend in air quality over time
SELECT startdate, avg(datavalue) 
FROM air_quality 
WHERE measure = "Mean" 
GROUP BY startdate 
ORDER BY SUBSTR(startdate, 4), SUBSTR(startdate, 0, 2);

--query to determine which area of NYC has the worst air quality on average over the scope of the data set
SELECT geoplacename, avg(datavalue) AS avgdata
FROM air_quality 
WHERE measure = "Mean" 
GROUP BY geoplacename
ORDER BY avgdata;