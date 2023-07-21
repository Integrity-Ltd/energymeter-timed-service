import declarations from "./declarations";
import moment from "moment";
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, `../${process.env.NODE_ENV ? process.env.NODE_ENV as string : ""}.env`) });

async function start() {
    const result = await declarations.yearlyProcess(moment([2023, 0, 1, 0, 30, 0]));
    console.log("aggregation result:", result);
}

start();