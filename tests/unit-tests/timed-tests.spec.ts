import "jest";
import hourlyProcess from "../../src/hourlydeclarations";
import yearlyProcess from "../../src/yearlydeclarations";
import moment from "moment";

jest.setTimeout(60000);
describe("Timed tests", () => {
    beforeEach((): void => {
    });

    test("Hourly process", async () => {
        const result = await hourlyProcess(moment([2023, 6, 21, 19, 0, 0]));
        expect(result).toEqual(true);
    })

    test("Yearly process", async () => {
        const result = await yearlyProcess(moment([2023, 0, 1, 0, 30, 0]));
        expect(result).toEqual(true);
    })
});