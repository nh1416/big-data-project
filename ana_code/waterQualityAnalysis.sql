USE tpm308;

DESCRIBE water_quality;

--query to determine if there is a trend in any of the tested parameters based on date
SELECT sampledate, avg(residualfreechlorine), avg(turbidity), avg(fluoride), avg(coliform), avg(ecoli) 
FROM water_quality 
GROUP BY sampledate 
ORDER BY SUBSTR(sampledate, 4), SUBSTR(sampledate, 0, 2);