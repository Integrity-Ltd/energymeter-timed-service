import declarations from "./declarations";
import moment from "moment";
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, `../${process.env.NODE_ENV ? process.env.NODE_ENV as string : ""}.env`) });

async function start() {
    const result = await declarations.hourlyProcess(moment([2023, 6, 21, 19, 0, 0]));
    console.log("generation result:", result);
}

start();