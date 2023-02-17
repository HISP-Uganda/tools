import * as dotenv from "dotenv";
import fs, { appendFileSync } from "fs";
import { parse } from "csv";
import axios from "axios";
import { chunk, slice } from "lodash";

dotenv.config();

let ids: string[][] = [];

// appendFileSync(
//   "./report.csv",
//   `id,isDuplicate,duplicates,exists,hasEnrollment,hasEvents,events,multipleEnrollments\n`
// );
const startIndex = 1129952;

const readData = async (
  sptIds: string[],
  fileName: string,
  currentIndex: number = 0
) => {
  const api = axios.create({
    baseURL: process.env.DHIS2_URL,
    auth: {
      username: process.env.DHIS2_USERNAME || "",
      password: process.env.DHIS2_PASSWORD || "",
    },
  });

  const allChunks = chunk(sptIds, 50);
  for (const ch of slice(allChunks, currentIndex)) {
    try {
      const { data } = await api.get("trackedEntityInstances", {
        params: {
          ouMode: "ALL",
          trackedEntityType: "MCPQUTHX1Ze",
          fields: "*",
          filter: `hDdStedsrHN:IN:${ch.join(";")}`,
          skipPaging: true,
        },
      });

      const found = data.trackedEntityInstances.flatMap(
        ({ attributes, enrollments }: any) => {
          const vaccinationCardNo = attributes.find(
            ({ attribute }: any) => attribute === "hDdStedsrHN"
          );
          if (vaccinationCardNo) {
            return { vaccinationCardNo: vaccinationCardNo.value, enrollments };
          }
          return [];
        }
      );
      ch.forEach((id) => {
        let obj = {
          id,
          isDuplicate: false,
          duplicates: 0,
          exists: false,
          hasEnrollment: false,
          hasEvents: false,
          events: 0,
          multipleEnrollments: false,
        };
        const search = found.filter(
          ({ vaccinationCardNo }: any) => vaccinationCardNo === id
        );
        if (search.length === 1) {
          const { enrollments } = search[0];
          obj = {
            ...obj,
            exists: true,
            hasEnrollment: enrollments.length > 0,
            multipleEnrollments: enrollments.length > 1,
            events: enrollments.flatMap(({ events }: any) =>
              events.map(({ event }: any) => event)
            ).length,
          };
        }
        if (search.length > 1) {
          obj = {
            ...obj,
            exists: true,
            duplicates: search.length,
            isDuplicate: true,
          };
        }
        appendFileSync(
          `./${fileName}.csv`,
          `${obj.id},${obj.isDuplicate},${obj.duplicates},${obj.exists},${obj.hasEnrollment},${obj.hasEvents},${obj.events},${obj.multipleEnrollments}\n`
        );
      });
      currentIndex += 1;
      console.log(`${fileName}-${currentIndex}`);
    } catch (error) {
      console.log(error);
    }
  }
};

fs.createReadStream("./spt_ids.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", (row) => {
    ids.push(row);
  })
  .on("end", async () => {
    const flattened = ids.flat();
    const finalChunks = flattened.slice(startIndex);
    const bigChunks = chunk(finalChunks, 50000);
    let ci = 0;
    for (const ch of bigChunks) {
      readData(ch, `report${ci}`);
      ci++;
    }
  })
  .on("error", (error) => {
    console.log(error.message);
  });
