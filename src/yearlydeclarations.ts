import moment from 'moment-timezone';
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import path from 'path';
import DBUtils from "../../energymeter-utils/src/utils/DBUtils";
import AdmZip from "adm-zip";
import fileLog from "../../energymeter-utils/src/utils/LogUtils";

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
    const fromDate = moment.tz(momentLastYear.get("year") + "-01-01", "YYYY-MM-DD", timeZone);
    const toDate = moment.tz((momentLastYear.get("year") + 1) + "-01-01", "YYYY-MM-DD", timeZone);

    const measurements: any[] = await DBUtils.getMeasurementsFromDBs(fromDate, toDate, IPAddess);
    const monthlyRecords: any[] = DBUtils.getDetails(measurements, timeZone, 'monthly', true);
    for await (const lastRec of monthlyRecords) {
        await DBUtils.runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
    };
    await cleanUpAggregatedFiles(IPAddess, momentLastYear);
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

export default yearlyProcess;