import dayjs from "dayjs";
import { Database } from 'sqlite3';
import sqlite3 from 'sqlite3';
import { runQuery, getMeasurementsFromEnergyMeter } from "../../energymeter-utils/src/utils/DBUtils";

/**
 * Hourly measure the power of each powermeter
 * 
 * @param currentTime time of hourly process
 * @returns true if process successfully done
 */
export async function hourlyProcess(currentTime: dayjs.Dayjs): Promise<boolean> {
    let result = true;
    try {
        let configDB = new Database(process.env.CONFIG_DB_FILE_NAME as string, sqlite3.OPEN_READONLY);
        let rows = await runQuery(configDB, 'SELECT * FROM energy_meter where enabled=1', []);
        for (const energymeter of rows) {
            try {
                let channels = await getActiveChannels(configDB, energymeter.id);
                let result = await getMeasurementsFromEnergyMeter(currentTime, energymeter, channels);
                console.log(dayjs().format(), "getMeasurementsFromEnergyMeter result: ", result);
            } catch (err) {
                console.error(dayjs().format(), err);
            }
        };
    } catch (err) {
        console.error(dayjs().format(), err);
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
    let channelsResult: any[] = await runQuery(configDB, `SELECT channel FROM channels WHERE energy_meter_id = ? and enabled=1`, [energyMeterId]);
    let channels: Array<String> = new Array<String>();
    channelsResult.map((ch) => {
        channels.push(ch.channel.toString());
    });
    return channels;
}
