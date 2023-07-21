import "jest";
import declarations from "../../src/declarations";
import moment from "moment";

describe("Timed tests", () => {
    beforeEach((): void => {
        jest.setTimeout(60000);

    });

    test("Hourly process", async () => {
        const result = await declarations.hourlyProcess(moment([2023, 6, 21, 19, 0, 0]));
        expect(result).toEqual(true);
    })

    test("Yearly process", async () => {
        const result = await declarations.yearlyProcess(moment([2023, 0, 1, 0, 30, 0]));
        expect(result).toEqual(true);
    })
});