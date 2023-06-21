import moment from 'moment';
import Net from 'net';
import cron from 'node-cron';
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config();

cron.schedule(process.env.YEARLY_CRONTAB as string, () => {
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
});

cron.schedule(process.env.HOURLY_CRONTAB as string, () => {
    let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
    let response = '';
    configDB.all('SELECT * FROM energy_meter where enabled=1', (err, rows) => {
        if (err) {
            console.log(moment().format(), `Error at selection: ${err}`);
        } else
            try {
                rows.forEach(async (row: any) => {
                    const client = new Net.Socket();
                    client.connect({ port: row.port, host: row.ip_address }, () => {
                        console.log(moment().format(), `TCP connection established with the server (${row.ip_address}).`);
                        client.write('read all');
                    });
                    client.on('data', function (chunk) {
                        response += chunk;
                        client.end();
                    });

                    client.on('end', async function () {
                        console.log(moment().format(), "Data received from the server.");
                        let db: Database | undefined = getMeasurementsDB(row.ip_address, moment().format("YYYY-MM") + '-monthly.sqlite', true);
                        if (!db) {
                            console.log(moment().format(), "No database exists.");
                            return;
                        }
                        try {
                            console.log(moment().format(), "Try lock DB.");
                            await runQuery(db, "BEGIN EXCLUSIVE", []);
                            let channels = await getActiveChannels(configDB, row.id);
                            processMeasurements(db, response, channels);
                        } catch (err) {
                            console.log(moment().format(), `DB access error: ${err}`);
                        }
                        finally {
                            try {
                                await runQuery(db, "COMMIT", []);
                            } catch (err) {
                                console.log(moment().format(), `Commit transaction error: ${err}`);
                            }
                            console.log(moment().format(), 'Closing DB connection...');
                            db.close();
                            console.log(moment().format(), 'DB connection closed.');
                            console.log(moment().format(), 'Closing TCP connection...');
                            client.destroy();
                            console.log(moment().format(), 'TCP connection destroyed.');
                        }

                    });
                });
            } catch (err) {
                console.log(moment().format(), err);
            }
    });
})

function runQuery(dbase: Database, sql: string, params: Array<any>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}

function getDBFilePath(IPAddress: string): string {
    const dbFilePath = (process.env.WORKDIR as string) + ((process.env.WORKDIR as string).endsWith(path.sep) ? '' : path.sep) + IPAddress;
    return dbFilePath;
}

function getMeasurementsDB(IPAddress: string, fileName: string, create: boolean): Database | undefined {
    let db: Database | undefined = undefined;
    const dbFilePath = getDBFilePath(IPAddress);
    if (!fs.existsSync(dbFilePath) && create) {
        fs.mkdirSync(dbFilePath, { recursive: true });
        console.log(moment().format(), `Directory '${dbFilePath}' created.`);
    }
    const dbFileName = dbFilePath + path.sep + fileName;
    if (!fs.existsSync(dbFileName)) {
        if (create) {
            db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            console.log(moment().format(), `DB file '${dbFileName}' created.`);
            db.exec(`CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`);
        }
    } else {
        console.log(moment().format(), `DB file '${dbFileName}' opened.`);
        db = new Database(dbFileName, sqlite3.OPEN_READWRITE);
    }
    return db;
}

function processMeasurements(db: Database, response: string, channels: String[]) {
    response.split('\n').forEach((line) => {
        let matches = line.match(/^channel_(\d{1,2}) : (.*)/);
        if (matches && channels.includes(matches[1])) {
            let measuredValue = parseFloat(matches[2]) * 1000;
            db.exec(`INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (${matches[1]}, ${measuredValue}, ${moment().unix()})`);
            console.log(moment().format(), matches[1], matches[2]);
        }
    });
}

async function getActiveChannels(configDB: Database, energyMeterId: number): Promise<Array<String>> {
    let channelsResult: any[] = await runQuery(configDB, `SELECT channel FROM channels WHERE energy_meter_id = ? and enabled=1`, [energyMeterId]);
    let channels: Array<String> = new Array<String>();
    channelsResult.map((ch) => {
        channels.push(ch.channel.toString());
    });
    return channels;
}

function processAggregation(rows: unknown[]) {
    rows.forEach(async (row: any) => {
        if (moment().month() == 0) {
            let momentLastYear = moment().add(-12, "months");
            let aggregatedDb: Database | undefined = getMeasurementsDB(row.ip_address, momentLastYear.format("YYYY") + '-yearly.sqlite', true);
            if (!aggregatedDb) {
                console.log(moment().format(), "Yearly aggregation database file not exists.");
            } else {
                aggregateDataLastYear(row.ip_address, aggregatedDb, momentLastYear);
            }
        }
    });
}

async function aggregateDataLastYear(IPAddess: string, aggregatedDb: Database, momentLastYear: moment.Moment) {
    for (let idx = 0; idx < 12; idx++) {
        const fileName = momentLastYear.format("YYYY-MM") + '-monthly.sqlite';
        let db: Database | undefined = getMeasurementsDB(IPAddess, fileName, false);
        if (db) {
            const firstRecords = await runQuery(db, "SELECT min(id) as id, channel, measured_value, recorded_time FROM Measurements group by channel", []);
            const lastRecords = await runQuery(db, "SELECT max(id) as id, channel, measured_value, recorded_time FROM Measurements group by channel", []);

            firstRecords.forEach(async (firstRec: any) => {
                await runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [firstRec.channel, firstRec.measured_value, firstRec.recorded_time]);
            })
            lastRecords.forEach(async (lastRec: any) => {
                await runQuery(aggregatedDb, "INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?,?,?)", [lastRec.channel, lastRec.measured_value, lastRec.recorded_time]);
            })
            db.close();
            if ((process.env.DELETE_FILE_AFTER_AGGREGATION as string) == "true") {
                const dbFilePath = getDBFilePath(IPAddess);
                const dbFileName = dbFilePath + path.sep + fileName;
                fs.rmSync(dbFileName);
            }
        }
        momentLastYear.add(1, "months");
    }
}

console.log(moment().format(), 'Server started.');