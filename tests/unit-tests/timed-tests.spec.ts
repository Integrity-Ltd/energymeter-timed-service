import "jest";
import { hourlyProcess } from "../../src/hourlydeclarations";
import { yearlyProcess } from "../../src/yearlydeclarations";
import dayjs from "dayjs";

jest.setTimeout(180000);
describe("Timed tests", () => {
    beforeEach((): void => {
    });

    test.skip("Hourly process", async () => {
        const date = new Date(2023, 6, 21, 19, 0, 0);
        const result = await hourlyProcess(dayjs(date));
        expect(result).toEqual(true);
    })

    test("Yearly process", async () => {
        const date = new Date(2023, 0, 1, 0, 30, 0);
        const result = await yearlyProcess(dayjs(date));
        expect(result).toEqual(true);
    })
});