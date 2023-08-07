import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import path from 'path';
import { runQuery, getDBFilePath, getMeasurementsDB, getMeasurementsFromDBs, getDetails } from "../../energymeter-utils/src/utils/DBUtils";
import AdmZip from "adm-zip";
//import fileLog from "../../energymeter-utils/src/utils/LogUtils";

dayjs.extend(utc)
dayjs.extend(timezone)

/**
 * Do the aggregation process
 * 
 * @param currentTime start time of yearly aggregation
 * @returns true if process successfully done
 */
export async function yearlyProcess(currentTime: dayjs.Dayjs): Promise<boolean> {
    let result = true;
    try {
        console.log(dayjs().format(), "Monthly aggregation started");
        let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
        let rows = await runQuery(configDB, 'SELECT * FROM energy_meter where enabled=1', []);
        try {
            await processAggregation(currentTime, rows);
        } catch (err) {
            result = false;
            console.log(dayjs().format(), `Error at aggregation: ${err}`);
        }
        configDB.close();
    } catch (err) {
        result = false;
        console.error(dayjs().format(), err);
    }
    return result;
}

/**
 * Aggregation process by powermeters
 * 
 * @param currentTime time of aggregation process
 * @param rows records of powermeter configurations
 */
async function processAggregation(currentTime: dayjs.Dayjs, rows: any[]) {
    let momentLastYear = dayjs(currentTime).add(-1, "year");
    for (const row of rows) {
        let aggregatedDb: Database | undefined = await getMeasurementsDB(row.ip_address, momentLastYear.format("YYYY") + '-yearly.sqlite', true);
        if (aggregatedDb) {
            try {
                await aggregateDataLastYear(row.ip_address, row.time_zone, aggregatedDb, momentLastYear)
            } catch (err) {
                console.error(dayjs().format(), row.ip_address, err);
            } finally {
                aggregatedDb.close()
            }
        } else {
            console.log(dayjs().format(), row.ip_address, "Yearly aggregation database file not exists.");
        }
    };
}

/**
 * Last year aggregation
 * @param IPAddess IP address of powermeter
 * @param timeZone Timezone of powermeter
 * @param aggregatedDb Created yearly aggregation database
 * @param momentLastYear The year of aggregation
 */
async function aggregateDataLastYear(IPAddess: string, timeZone: string, aggregatedDb: Database, momentLastYear: dayjs.Dayjs) {
    const from = momentLastYear.get("year") + "-01-01";
    const fromDate = dayjs.tz(from, "YYYY-MM-DD", timeZone);
    const to = (momentLastYear.get("year") + 1) + "-01-01";
    const toDate = dayjs.tz(to, "YYYY-MM-DD", timeZone);

    const measurements: any[] = await getMeasurementsFromDBs(fromDate, toDate, IPAddess);
    const monthlyRecords: any[] = getDetails(measurements, timeZone, 'monthly', true);
    runQuery(aggregatedDb, "BEGIN", []);
    for await (const lastRec of monthlyRecords) {
        await runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
    };
    runQuery(aggregatedDb, "COMMIT", []);
    await cleanUpAggregatedFiles(IPAddess, momentLastYear);
}

async function archiveLastYear(dbFilesPath: string, archiveRelativeFilePath: string, lastYear: dayjs.Dayjs) {
    const year = lastYear.year();
    const outPath = path.join(dbFilesPath, archiveRelativeFilePath);
    if (!fs.existsSync(outPath)) {
        fs.mkdirSync(outPath, { recursive: true });
        console.log(dayjs().format(), `Directory '${outPath}' created.`);
    }
    await compressFiles(year, dbFilesPath, path.join(outPath, `${year}.zip`));
}

/**
 * Archive and remove last year of datas
 * @param IPAddess IP address of powermeter
 * @param momentLastYear last year of aggregation for search SQLite files
 */
async function cleanUpAggregatedFiles(IPAddess: string, momentLastYear: dayjs.Dayjs) {
    const dbFilePath = getDBFilePath(IPAddess);
    const archiveRelativeFilePath = process.env.ARCHIVE_FILE_PATH as string;
    try {
        await archiveLastYear(dbFilePath, archiveRelativeFilePath, momentLastYear);
        if ((process.env.DELETE_FILE_AFTER_AGGREGATION as string) == "true") {
            let monthlyIterator = dayjs(momentLastYear);
            for (let idx = 0; idx < 12; idx++) {
                const fileName = monthlyIterator.format("YYYY-MM") + '-monthly.sqlite';
                const dbFileName = path.join(dbFilePath, fileName);
                if (fs.existsSync(dbFileName)) {
                    fs.rmSync(dbFileName);
                }
                monthlyIterator = monthlyIterator.add(1, "months");
            }
        }
    } catch (err) {
        console.log(dayjs().format(), IPAddess, err);
    }
}

/**
 * 
 * @param year year part of monthly SQLite file
 * @param dbFilesPath the SQLite files path that will be compressed
 * @param destination the compressed file name with path
 * @returns true if compression succesfully done
 */
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
                        console.log(dayjs().format(), "added file to zip:", file);
                        zip.addLocalFile(fileName);
                    }
                }
            });
            zip.writeZip(destination);
            console.log(dayjs().format(), "Zip created:", destination)
            resolve(true);
        } catch (err) {
            console.error(dayjs().format(), 'Unable to scan directory: ' + err);
            reject(err);
        }
    });
}
