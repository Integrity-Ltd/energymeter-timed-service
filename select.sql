SELECT STRFTIME('%Y-%m-%d, %H:%M:%S',DATETIME(recorded_time,'unixepoch','localtime')) as formatted_record_time, channel, measured_value FROM Measurements;

SELECT * FROM Measurements where channel = 1 order by recorded_time DESC;

SELECT max(id), channel, measured_value, recorded_time FROM Measurements group by channel;