import moment from 'moment-timezone';
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import DBUtils from "../../energymeter-utils/src/utils/DBUtils";

/**
 * Hourly measure the power of each powermeter
 * 
 * @param currentTime time of hourly process
 * @returns true if process successfully done
 */
export async function hourlyProcess(currentTime: moment.Moment): Promise<boolean> {
    let result = true;
    try {
        let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
        let rows = await DBUtils.runQuery(configDB, 'SELECT * FROM energy_meter where enabled=1', []);
        for (const energymeter of rows) {
            try {
                let channels = await getActiveChannels(configDB, energymeter.id);
                let result = await DBUtils.getMeasurementsFromEnergyMeter(currentTime, energymeter, channels);
                console.log(moment().format(), "getMeasurementsFromEnergyMeter result: ", result);
            } catch (err) {
                console.error(moment().format(), err);
            }
        };
    } catch (err) {
        console.error(moment().format(), err);
    }
    return result;
}

/**
 * Get active channels of powermeter
 * @param configDB the SQLite config file of powermeters
 * @param energyMeterId the ID of powermeter
 * @returns 
 */
async function getActiveChannels(configDB: Database, energyMeterId: number): Promise<Array<String>> {
    let channelsResult: any[] = await DBUtils.runQuery(configDB, `SELECT channel FROM channels WHERE energy_meter_id = ? and enabled=1`, [energyMeterId]);
    let channels: Array<String> = new Array<String>();
    channelsResult.map((ch) => {
        channels.push(ch.channel.toString());
    });
    return channels;
}
