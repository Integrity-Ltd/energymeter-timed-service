import moment, { MomentTimezone } from 'moment-timezone';
import cron from 'node-cron';
import fs from 'fs';
import dotenv from 'dotenv';
import path from 'path';
import declarations from './declarations';

dotenv.config({ path: path.resolve(__dirname, `../${process.env.NODE_ENV ? process.env.NODE_ENV as string : ""}.env`) });

if (process.env.NODE_ENV === "docker" && !fs.existsSync(path.join(process.env.WORKDIR as string, "config.sqlite"))) {
    fs.copyFileSync(path.resolve(__dirname, "../config.sqlite"), path.join(process.env.WORKDIR as string, "config.sqlite"));
}

cron.schedule(process.env.YEARLY_CRONTAB as string, () => {
    let currentTime = moment();
    if (currentTime.month() == 0) {
        declarations.yearlyProcess(currentTime);
    }
});

cron.schedule(process.env.HOURLY_CRONTAB as string, () => {
    declarations.hourlyProcess(moment());
})


console.log(moment().format(), 'Server started.');
