CREATE TABLE IF NOT EXISTS Power_Consumption (
    experiment_id VARCHAR,
    node_id VARCHAR,
    node_id_seq VARCHAR,
    time_sec VARCHAR NOT NULL,
    time_usec VARCHAR NOT NULL,
    power REAL NOT NULL,
    current REAL NOT NULL,
    voltage REAL NOT NULL
);

COPY Power_Consumption FROM '/Users/htl719/Dropbox/Class/ADS-23/livecoding/st_lrwan1_11_oml.csv' 
(AUTO_DETECT TRUE);


CREATE SEQUENCE IF NOT EXISTS Power_Consumption_id_seq;

CREATE VIEW PC AS (SELECT nextval('power_consumption_id_seq') AS id, 
                          cast(time_sec AS real) + cast(time_usec AS real) AS ts,
	                  power, current, voltage
	           FROM power_consumption);
SELECT * FROM pc;	

