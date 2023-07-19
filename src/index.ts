import moment, { MomentTimezone } from 'moment-timezone';
import Net from 'net';
import cron from 'node-cron';
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import dotenv from 'dotenv';
import path from 'path';
import targz from 'targz';
import DBUtils from "../../energymeter-utils/src/utils/DBUtils";
dotenv.config({ path: path.resolve(__dirname, `../${process.env.NODE_ENV ? process.env.NODE_ENV as string : ""}.env`) });

if (process.env.NODE_ENV === "docker" && !fs.existsSync(path.join(process.env.WORKDIR as string, "config.sqlite"))) {
    fs.copyFileSync(path.resolve(__dirname, "../config.sqlite"), path.join(process.env.WORKDIR as string, "config.sqlite"));
}
/*
cron.schedule(process.env.YEARLY_ARCHIVE_CRONTAB as string, () => {
    let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
    const momentLastYear = moment().add(-12, "months");
    configDB.all('SELECT * FROM energy_meter where enabled=1', (err, rows) => {
        if (err) {
            console.log(moment().format(), `Error at selection: ${err}`);
        } else {
            rows.forEach(async (row: any) => {
                try {
                    cleanUpAggregatedFiles(row.ip_address, momentLastYear);
                } catch (err) {
                    console.error(moment().format(), `Error at aggregation: ${err}`);
                }
            });
        }
    });
})
*/

cron.schedule(process.env.YEARLY_CRONTAB as string, () => {
    if (moment().month() == 0)
        try {
            console.log(moment().format(), "Monthly aggregation started");
            let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
            configDB.all('SELECT * FROM energy_meter where enabled=1', async (err, rows) => {
                if (err) {
                    console.log(moment().format(), `Error at selection: ${err}`);
                } else
                    try {
                        processAggregation(rows);
                    } catch (err) {
                        console.log(moment().format(), `Error at aggregation: ${err}`);
                    }
            });
        } catch (err) {
            console.error(moment().format(), err);
        }
});

cron.schedule(process.env.HOURLY_CRONTAB as string, () => {
    try {
        let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
        configDB.all('SELECT * FROM energy_meter where enabled=1', (err, rows) => {
            if (err) {
                console.log(moment().format(), `Error at selection: ${err}`);
            } else
                try {
                    rows.forEach(async (energymeter: any) => {
                        let channels = await getActiveChannels(configDB, energymeter.id);
                        getMeasurementsFromEnergyMeter(energymeter, channels);
                    });
                } catch (err) {
                    console.error(moment().format(), err);
                }
        });
    } catch (err) {
        console.error(moment().format(), err);
    }
})

async function getActiveChannels(configDB: Database, energyMeterId: number): Promise<Array<String>> {
    let channelsResult: any[] = await DBUtils.runQuery(configDB, `SELECT channel FROM channels WHERE energy_meter_id = ? and enabled=1`, [energyMeterId]);
    let channels: Array<String> = new Array<String>();
    channelsResult.map((ch) => {
        channels.push(ch.channel.toString());
    });
    return channels;
}

function processAggregation(rows: unknown[]) {
    rows.forEach(async (row: any) => {
        let momentLastYear = moment().add(-1, "year");
        let aggregatedDb: Database | undefined = await DBUtils.getMeasurementsDB(row.ip_address, momentLastYear.format("YYYY") + '-yearly.sqlite', true);
        if (aggregatedDb) {
            aggregateDataLastYear(row.ip_address, row.time_zone, aggregatedDb, momentLastYear).then(() => {
                aggregatedDb?.close();
            }).catch((err) => {
                console.error(moment().format(), row.ip_address, err);
                aggregatedDb?.close();
            });
        } else {
            console.log(moment().format(), row.ip_address, "Yearly aggregation database file not exists.");
        }
    });
}

async function aggregateDataLastYear(IPAddess: string, timeZone: string, aggregatedDb: Database, momentLastYear: moment.Moment) {
    let firstDay = moment(momentLastYear).set("month", 0).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
    let lastDay = moment(firstDay).add(1, "year");
    const lastRecords: any[] = await getMonthlyMeasurements(firstDay, lastDay, IPAddess, timeZone);
    lastRecords.forEach(async (lastRec: any) => {
        await DBUtils.runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
    });
    cleanUpAggregatedFiles(IPAddess, momentLastYear);
}

async function getMonthlyMeasurements(fromDate: moment.Moment, toDate: moment.Moment, ip: string, timeZone: string): Promise<any[]> {
    let measurements = await getMeasurementsFromDBs(fromDate, toDate, ip);
    let result: any[] = [];
    let prevElement: any = {};
    let lastElement: any = {};
    let isMonthlyEnabled = false;
    measurements.forEach((element: any, idx: number) => {
        if (prevElement[element.channel] == undefined) {
            prevElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel, diff: 0 };
            result.push({ ...prevElement[element.channel] });
        } else {
            const roundedPrevMonth = moment.unix(prevElement[element.channel].recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0).tz(timeZone);
            const roundedMonth = moment.unix(element.recorded_time).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0).tz(timeZone);
            const diffMonths = roundedMonth.diff(roundedPrevMonth, "months");
            isMonthlyEnabled = diffMonths >= 1;

            if (isMonthlyEnabled) {
                prevElement[element.channel] = {
                    recorded_time: element.recorded_time,
                    measured_value: element.measured_value,
                    channel: element.channel,
                };
                result.push({ ...prevElement[element.channel] });
            }

            lastElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel };
        }
    });

    if (!isMonthlyEnabled) {
        Object.keys(lastElement).forEach((key) => {
            try {
                const diff = lastElement[key].measured_value - prevElement[lastElement[key].channel].measured_value;
                prevElement[lastElement[key].channel] = {
                    recorded_time: lastElement[key].recorded_time,
                    measured_value: lastElement[key].measured_value,
                    channel: lastElement[key].channel
                };
                if (diff != 0) {
                    result.push({ ...prevElement[lastElement[key].channel] });
                }
            } catch (err) {
                console.error(moment().format(), err);
            }
        });
    }
    return result;
}

async function getMeasurementsFromDBs(fromDate: moment.Moment, toDate: moment.Moment, ip: string): Promise<any[]> {
    let monthlyIterator = moment(fromDate).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0);
    let result: any[] = [];
    while (monthlyIterator.isBefore(toDate) || monthlyIterator.isSame(toDate)) {
        const filePath = (process.env.WORKDIR as string);
        const dbFile = path.join(filePath, ip, monthlyIterator.format("YYYY-MM") + "-monthly.sqlite");
        if (fs.existsSync(dbFile)) {
            const db = new Database(dbFile);
            try {
                const fromSec = fromDate.unix();
                const toSec = toDate.unix();
                let measurements = await DBUtils.runQuery(db, "select * from measurements where recorded_time between ? and ? order by recorded_time, channel", [fromSec, toSec]);
                measurements.forEach((element: any) => {
                    result.push(element);
                })
            } catch (err) {
                console.error(moment().format(), err);
            } finally {
                db.close();
            }
        }
        monthlyIterator.add(1, "months");
    }

    return result;
}

async function archiveLastYear(dbFilesPath: string, archiveRelativeFilePath: string, lastYear: moment.Moment) {
    const year = lastYear.year();
    const outPath = path.join(dbFilesPath, archiveRelativeFilePath);
    if (!fs.existsSync(outPath)) {
        fs.mkdirSync(outPath, { recursive: true });
        console.log(moment().format(), `Directory '${outPath}' created.`);
    }
    await compressFiles(year, dbFilesPath, path.join(outPath, `${year}.tgz`));
}

function cleanUpAggregatedFiles(IPAddess: string, momentLastYear: moment.Moment) {
    const dbFilePath = DBUtils.getDBFilePath(IPAddess);
    const archiveRelativeFilePath = process.env.ARCHIVE_FILE_PATH as string;
    archiveLastYear(dbFilePath, archiveRelativeFilePath, momentLastYear).then(() => {
        if ((process.env.DELETE_FILE_AFTER_AGGREGATION as string) == "true") {
            let monthlyIterator = moment(momentLastYear);
            for (let idx = 0; idx < 12; idx++) {
                const fileName = monthlyIterator.format("YYYY-MM") + '-monthly.sqlite';
                const dbFileName = path.join(dbFilePath, fileName);
                fs.rmSync(dbFileName);
                monthlyIterator.add(1, "months");
            }
        }
    }).catch((err) => {
        console.log(moment().format(), IPAddess, err);
    })
}

function getMeasurementsFromEnergyMeter(energymeter: any, channels: any) {
    let response = '';
    const client = new Net.Socket();
    client.setTimeout(5000);
    try {
        client.connect({ port: energymeter.port, host: energymeter.ip_address }, () => {
            console.log(moment().format(), energymeter.ip_address, `TCP connection established with the server.`);
            client.write('read all');
        });
    } catch (err) {
        console.error(moment().format(), energymeter.ip_address, err);
    }
    client.on('timeout', function () {
        console.error(moment().format(), energymeter.ip_address, "Connection timeout");
    });
    client.on('error', function (err) {
        console.error(moment().format(), energymeter.ip_address, err);
    });
    client.on('data', function (chunk) {
        response += chunk;
        client.end();
    });

    client.on('end', async function () {
        console.log(moment().format(), energymeter.ip_address, "Data received from the server.");
        let db: Database | undefined = await DBUtils.getMeasurementsDB(energymeter.ip_address, moment().format("YYYY-MM") + '-monthly.sqlite', true);
        if (!db) {
            console.error(moment().format(), energymeter.ip_address, "No database exists.");
            return;
        }
        try {
            console.log(moment().format(), energymeter.ip_address, "Try lock DB.");
            await DBUtils.runQuery(db, "BEGIN EXCLUSIVE", []);
            DBUtils.processMeasurements(db, energymeter.ip_address, response, channels);
        } catch (err) {
            console.log(moment().format(), energymeter.ip_address, `DB access error: ${err}`);
        }
        finally {
            try {
                await DBUtils.runQuery(db, "COMMIT", []);
            } catch (err) {
                console.log(moment().format(), energymeter.ip_address, `Commit transaction error: ${err}`);
            }
            console.log(moment().format(), energymeter.ip_address, 'Closing DB connection...');
            db.close();
            console.log(moment().format(), energymeter.ip_address, 'DB connection closed.');
            console.log(moment().format(), energymeter.ip_address, 'Closing TCP connection...');
            client.destroy();
            console.log(moment().format(), energymeter.ip_address, 'TCP connection destroyed.');
        }

    });
}

function compressFiles(year: number, dbFilesPath: string, destination: string) {
    const pattern = `${year}-\\d+-monthly.sqlite$`;
    return new Promise<any>((resolve, reject) => {
        targz.compress({
            src: dbFilesPath,
            dest: destination,
            tar: {
                ignore: function (name) {
                    const mresult = name.match(pattern);
                    return mresult == null || mresult.length == 0;
                }
            },
            gz: {
                level: 6,
                memLevel: 6
            }
        }, function (err) {
            if (err) {
                console.error(err);
                return reject(err);
            } else {
                console.log(moment().format(), destination, "Archive created.");
                return resolve(true);
            }
        });
    });
}

console.log(moment().format(), 'Server started.');