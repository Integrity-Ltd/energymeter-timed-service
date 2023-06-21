"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const moment_1 = __importDefault(require("moment"));
const net_1 = __importDefault(require("net"));
const node_cron_1 = __importDefault(require("node-cron"));
const sqlite3_1 = require("sqlite3");
const sqlite3_2 = __importDefault(require("sqlite3"));
const fs_1 = __importDefault(require("fs"));
const dotenv_1 = __importDefault(require("dotenv"));
const path_1 = __importDefault(require("path"));
dotenv_1.default.config();
node_cron_1.default.schedule(process.env.HOURLY_CRONTAB, () => {
    let configDB = new sqlite3_1.Database(process.env.CONFIG_DB_FILE_NAME, sqlite3_2.default.OPEN_READONLY);
    let response = '';
    configDB.all('SELECT * FROM energy_meter where enabled=1', (err, rows) => {
        if (err) {
            console.log(`${(0, moment_1.default)().format()} Error: ${err}`);
        }
        else
            try {
                rows.forEach((row) => __awaiter(void 0, void 0, void 0, function* () {
                    const client = new net_1.default.Socket();
                    client.connect({ port: row.port, host: row.ip_address }, () => {
                        console.log((0, moment_1.default)().format(), `TCP connection established with the server (${row.ip_address}).`);
                        client.write('read all');
                    });
                    client.on('data', function (chunk) {
                        response += chunk;
                        client.end();
                    });
                    client.on('end', function () {
                        return __awaiter(this, void 0, void 0, function* () {
                            console.log((0, moment_1.default)().format(), "Data received from the server.");
                            let db = getMeasurementsDB(row.ip_address);
                            try {
                                console.log((0, moment_1.default)().format(), "Try lock DB.");
                                yield runQuery(db, "BEGIN EXCLUSIVE", []);
                                let channels = yield getActiveChannels(configDB, row.id);
                                processMeasurements(db, response, channels);
                            }
                            catch (err) {
                                console.log((0, moment_1.default)().format(), `DB access error: ${err}`);
                            }
                            finally {
                                try {
                                    yield runQuery(db, "COMMIT", []);
                                }
                                catch (err) {
                                    console.log((0, moment_1.default)().format(), `Commit transaction error: ${err}`);
                                }
                                console.log((0, moment_1.default)().format(), 'Closing DB connection...');
                                db.close();
                                console.log((0, moment_1.default)().format(), 'DB connection closed.');
                                console.log((0, moment_1.default)().format(), 'Closing TCP connection...');
                                client.destroy();
                                console.log((0, moment_1.default)().format(), 'TCP connection destroyed.');
                            }
                        });
                    });
                }));
            }
            catch (err) {
                console.log((0, moment_1.default)().format(), err);
            }
    });
});
function runQuery(dbase, sql, params) {
    return new Promise((resolve, reject) => {
        return dbase.all(sql, params, (err, res) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}
function getMeasurementsDB(IPAddress) {
    let dailyFile = (0, moment_1.default)().format("YYYY-MM") + '-monthly.sqlite';
    let db;
    const dbFilePath = process.env.WORKDIR + (process.env.WORKDIR.endsWith(path_1.default.sep) ? '' : path_1.default.sep) + IPAddress;
    if (!fs_1.default.existsSync(dbFilePath)) {
        fs_1.default.mkdirSync(dbFilePath, { recursive: true });
        console.log((0, moment_1.default)().format(), `Directory '${dbFilePath}' created.`);
    }
    const dbFileName = dbFilePath + path_1.default.sep + dailyFile;
    if (!fs_1.default.existsSync(dbFileName)) {
        db = new sqlite3_1.Database(dbFileName, sqlite3_2.default.OPEN_READWRITE | sqlite3_2.default.OPEN_CREATE);
        console.log((0, moment_1.default)().format(), `DB file '${dbFileName}' created.`);
        db.exec(`CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`);
    }
    else {
        console.log((0, moment_1.default)().format(), `DB file '${dbFileName}' opened.`);
        db = new sqlite3_1.Database(dbFileName, sqlite3_2.default.OPEN_READWRITE);
    }
    return db;
}
function processMeasurements(db, response, channels) {
    response.split('\n').forEach((line) => {
        let matches = line.match(/^channel_(\d{1,2}) : (.*)/);
        if (matches && channels.includes(matches[1])) {
            let measuredValue = parseFloat(matches[2]) * 1000;
            db.exec(`INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (${matches[1]}, ${measuredValue}, ${(0, moment_1.default)().unix()})`);
            console.log((0, moment_1.default)().format(), matches[1], matches[2]);
        }
    });
}
function getActiveChannels(configDB, energyMeterId) {
    return __awaiter(this, void 0, void 0, function* () {
        let channelsResult = yield runQuery(configDB, `SELECT channel FROM channels WHERE energy_meter_id = ? and enabled=1`, [energyMeterId]);
        let channels = new Array();
        channelsResult.map((ch) => {
            channels.push(ch.channel.toString());
        });
        return channels;
    });
}
console.log((0, moment_1.default)().format(), 'Server started.');
