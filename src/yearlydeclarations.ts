import moment from 'moment-timezone';
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import path from 'path';
import DBUtils from "../../energymeter-utils/src/utils/DBUtils";
import AdmZip from "adm-zip";

async function yearlyProcess(currentTime: moment.Moment): Promise<boolean> {
    let result = true;
    try {
        console.log(moment().format(), "Monthly aggregation started");
        let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
        let rows = await DBUtils.runQuery(configDB, 'SELECT * FROM energy_meter where enabled=1', []);
        try {
            await processAggregation(currentTime, rows);
        } catch (err) {
            result = false;
            console.log(moment().format(), `Error at aggregation: ${err}`);
        }
        configDB.close();
    } catch (err) {
        result = false;
        console.error(moment().format(), err);
    }
    return result;
}

async function processAggregation(currentTime: moment.Moment, rows: any[]) {
    let momentLastYear = moment(currentTime).add(-1, "year");
    for (const row of rows) {
        let aggregatedDb: Database | undefined = await DBUtils.getMeasurementsDB(row.ip_address, momentLastYear.format("YYYY") + '-yearly.sqlite', true);
        if (aggregatedDb) {
            try {
                await aggregateDataLastYear(row.ip_address, row.time_zone, aggregatedDb, momentLastYear)
            } catch (err) {
                console.error(moment().format(), row.ip_address, err);
            } finally {
                aggregatedDb.close()
            }
        } else {
            console.log(moment().format(), row.ip_address, "Yearly aggregation database file not exists.");
        }
    };
}

async function aggregateDataLastYear(IPAddess: string, timeZone: string, aggregatedDb: Database, momentLastYear: moment.Moment) {
    let firstDay = moment(momentLastYear).set("month", 0).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
    let lastDay = moment(firstDay).add(1, "year");
    const lastRecords: any[] = await getMonthlyMeasurements(firstDay, lastDay, IPAddess, timeZone);
    for await (const lastRec of lastRecords) {
        await DBUtils.runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
    };
    await cleanUpAggregatedFiles(IPAddess, momentLastYear);
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

async function archiveLastYear(dbFilesPath: string, archiveRelativeFilePath: string, lastYear: moment.Moment) {
    const year = lastYear.year();
    const outPath = path.join(dbFilesPath, archiveRelativeFilePath);
    if (!fs.existsSync(outPath)) {
        fs.mkdirSync(outPath, { recursive: true });
        console.log(moment().format(), `Directory '${outPath}' created.`);
    }
    await compressFiles(year, dbFilesPath, path.join(outPath, `${year}.zip`));
}

async function cleanUpAggregatedFiles(IPAddess: string, momentLastYear: moment.Moment) {
    const dbFilePath = DBUtils.getDBFilePath(IPAddess);
    const archiveRelativeFilePath = process.env.ARCHIVE_FILE_PATH as string;
    try {
        await archiveLastYear(dbFilePath, archiveRelativeFilePath, momentLastYear);
        if ((process.env.DELETE_FILE_AFTER_AGGREGATION as string) == "true") {
            let monthlyIterator = moment(momentLastYear);
            for (let idx = 0; idx < 12; idx++) {
                const fileName = monthlyIterator.format("YYYY-MM") + '-monthly.sqlite';
                const dbFileName = path.join(dbFilePath, fileName);
                if (fs.existsSync(dbFileName)) {
                    fs.rmSync(dbFileName);
                }
                monthlyIterator.add(1, "months");
            }
        }
    } catch (err) {
        console.log(moment().format(), IPAddess, err);
    }
}

function compressFiles(year: number, dbFilesPath: string, destination: string): Promise<any> {
    return new Promise<any>((resolve, reject) => {
        const pattern = `${year}-\\d+-monthly.sqlite$`;
        const zip = new AdmZip();
        try {
            let files = fs.readdirSync(dbFilesPath)
            files.forEach(function (file) {
                const mresult = file.match(pattern);
                if (mresult) {
                    const fileName = path.join(dbFilesPath, file);
                    if (fs.lstatSync(fileName).isFile()) {
                        console.log(moment().format(), "added file to zip:", file);
                        zip.addLocalFile(fileName);
                    }
                }
            });
            zip.writeZip(destination);
            console.log(moment().format(), "Zip created:", destination)
            resolve(true);
        } catch (err) {
            console.error(moment().format(), 'Unable to scan directory: ' + err);
            reject(err);
        }
    });
}

async function getMeasurementsFromDBs(fromDate: moment.Moment, toDate: moment.Moment, ip: string): Promise<any[]> {
    let monthlyIterator = moment(fromDate).set("date", 1).set("hour", 0).set("minute", 0).set("second", 0).set("millisecond", 0);
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

export default yearlyProcess;