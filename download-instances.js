import axios from "axios";
import { Parser } from "@json2csv/plainjs";

import { writeFileSync } from "fs";

import * as dotenv from "dotenv";
import _ from "lodash";

const { fromPairs } = _;

dotenv.config();

const api = axios.create({
  baseURL: process.env.DHIS2_URL,
  auth: {
    username: process.env.DHIS2_USERNAME || "",
    password: process.env.DHIS2_PASSWORD || "",
  },
});

const download = async (startPage = 1, pageSize = 500) => {
  let instances = [];
  const {
    data: { trackedEntityInstances, ...rest },
  } = await api.get("trackedEntityInstances", {
    params: {
      ouMode: "ALL",
      program: "yDuAzyqYABS",
      fields: "attributes",
      pageSize,
      page: startPage,
      totalPages: true,
    },
  });

  const processed = trackedEntityInstances.map(({ attributes }) =>
    fromPairs(attributes.map(({ displayName, value }) => [displayName, value]))
  );

  instances = instances.concat(processed);

  for (let page = startPage + 1; page <= rest.pager.pageCount; page++) {
    console.log(`Woking on page ${page}`);
    const {
      data: { trackedEntityInstances },
    } = await api.get("trackedEntityInstances", {
      params: {
        ouMode: "ALL",
        program: "yDuAzyqYABS",
        fields: "attributes",
        pageSize,
        page,
      },
    });

    const processed = trackedEntityInstances.map(({ attributes }) =>
      fromPairs(
        attributes.map(({ displayName, value }) => [displayName, value])
      )
    );

    if (instances.length >= 10000) {
      console.log(`Saving intermediate data`);
      const parser = new Parser({ defaultValue: "", includeEmptyRows: true });
      const csv = parser.parse(instances);
      writeFileSync(`chunk${page}.csv`, csv);
      instances = processed;
    } else {
      instances = instances.concat(processed);
    }
  }
};

download().then(() => console.log("Done"));
