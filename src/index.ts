import moment from 'moment';
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

if (process.env.NODE_ENV === "docker" && !fs.existsSync(process.env.WORKDIR + path.sep + "config.sqlite")) {
    fs.copyFileSync(path.resolve(__dirname, "../config.sqlite"), process.env.WORKDIR + path.sep + "config.sqlite");
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
            configDB.all('SELECT * FROM energy_meter where enabled=1', (err, rows) => {
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
            aggregateDataLastYear(row.ip_address, aggregatedDb, momentLastYear).then(() => {
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

async function aggregateDataLastYear(IPAddess: string, aggregatedDb: Database, momentLastYear: moment.Moment) {
    let firstDay = moment(momentLastYear).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
    let lastDay = moment(firstDay).add(1, "year");
    const lastRecords: any[] = await getMonthlyMeasurements(firstDay, lastDay, IPAddess);
    lastRecords.forEach(async (lastRec: any) => {
        await DBUtils.runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
    });
    cleanUpAggregatedFiles(IPAddess, momentLastYear);
}

async function getMonthlyMeasurements(fromDate: moment.Moment, toDate: moment.Moment, ip: string): Promise<any[]> {
    let measurements = await getMeasurementsFromDBs(fromDate, toDate, ip);
    let result: any[] = [];
    let prevElement: any = {};
    let lastElement: any = {};
    let isMonthlyEnabled = false;
    measurements.forEach((element: any, idx: number) => {
        if (prevElement[element.channel] == undefined) {
            prevElement[element.channel] = { recorded_time: element.recorded_time, measured_value: element.measured_value, channel: element.channel, diff: 0 };
        } else {
            const roundedPrevMonth = moment.unix(prevElement[element.channel].recorded_time).utc().set("date", 1).set("hour", 0).set("minute", 0).set("second", 0);
            const roundedMonth = moment.unix(element.recorded_time).utc().set("date", 1).set("hour", 0).set("minute", 0).set("second", 0);
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
    let monthlyIterator = moment(fromDate);
    let result: any[] = [];
    while (monthlyIterator.isBefore(toDate)) {
        const filePath = (process.env.WORKDIR as string);
        const dbFile = filePath + (filePath.endsWith(path.sep) ? "" : path.sep) + ip + path.sep + monthlyIterator.format("YYYY-MM") + "-monthly.sqlite";
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

    if (result.length > 0) {
        const lastRecordedTime = result[result.length - 1].recorded_time;
        let nextHour = moment.unix(lastRecordedTime);
        nextHour.add(1, "hour");
        const filePath = (process.env.WORKDIR as string);
        const dbFile = filePath + (filePath.endsWith(path.sep) ? "" : path.sep) + ip + path.sep + nextHour.format("YYYY-MM") + "-monthly.sqlite";
        if (fs.existsSync(dbFile)) {
            const db = new Database(dbFile);
            try {
                const firstRecords = await DBUtils.runQuery(db, "SELECT min(id) as id, channel, measured_value, recorded_time FROM measurements where recorded_time > ? group by channel", [lastRecordedTime]);
                firstRecords.forEach((element: any) => {
                    result.push(element);
                })
            } catch (err) {
                console.error(moment().format(), err);
            } finally {
                db.close();
            }
        }
    }
    return result;
}

function archiveLastYear(dbFilesPath: string, archiveRelativeFilePath: string, lastYear: moment.Moment) {
    const year = lastYear.year();
    const pattern = `${year}-\\d+-monthly.sqlite$`;
    const outPath = dbFilesPath + path.sep + archiveRelativeFilePath;
    if (!fs.existsSync(outPath)) {
        fs.mkdirSync(outPath, { recursive: true });
        console.log(moment().format(), `Directory '${outPath}' created.`);
    }
    targz.compress({
        src: dbFilesPath,
        dest: outPath + path.sep + `${year}.tgz`,
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
        } else {
            console.log(moment().format(), "Archive created.");
        }
    });
}

function cleanUpAggregatedFiles(IPAddess: string, momentLastYear: moment.Moment) {
    const dbFilePath = DBUtils.getDBFilePath(IPAddess);
    const archiveRelativeFilePath = process.env.ARCHIVE_FILE_PATH as string;
    archiveLastYear(dbFilePath, archiveRelativeFilePath, momentLastYear);
    if ((process.env.DELETE_FILE_AFTER_AGGREGATION as string) == "true") {
        let monthlyIterator = moment(momentLastYear);
        for (let idx = 0; idx < 12; idx++) {
            const fileName = monthlyIterator.format("YYYY-MM") + '-monthly.sqlite';
            const dbFileName = dbFilePath + path.sep + fileName;
            fs.rmSync(dbFileName);
            monthlyIterator.add(1, "months");
        }
    }
}

function getMeasurementsFromEnergyMeter(energymeter: any, channels: any) {
    let response = '';
    const client = new Net.Socket();
    try {
        client.connect({ port: energymeter.port, host: energymeter.ip_address }, () => {
            console.log(moment().format(), energymeter.ip_address, `TCP connection established with the server.`);
            client.write('read all');
        });
    } catch (err) {
        console.error(moment().format(), err);
    }
    client.on('error', function (err) {
        console.error(moment().format(), err);
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
            DBUtils.processMeasurements(db, response, channels);
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

console.log(moment().format(), 'Server started.');
