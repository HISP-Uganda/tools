import { AsyncParser } from "@json2csv/node";
import mergeByKey from "array-merge-by-key";
import axios from "axios";
import { writeFileSync } from "fs";
import _ from "lodash";

import defenceSites from "./defenceSites.json" assert { type: "json" };

const { chunk, fromPairs, groupBy, orderBy, pick, uniq } = _;
const PROGRAM = "yDuAzyqYABS";
const NIN_ATTRIBUTE = "Ewi7FUfcHAD";
const OTHER_ID = "YvnFn4IjKzx";
const PHONE_ATTRIBUTE = "ciCR6BBvIT4";
const DOSE_PLACE = "AmTw4pWCCaJ";
const ELSEWHERE_IN_COUNTRY_DISTRICT = "ObwW38YrQHu";
const ELSEWHERE_IN_COUNTRY_FACILITY = "X7tI86pr1y0";
const ELSEWHERE_OUT_COUNTRY_FACILITY = "OW3erclrDW8";
const ELSEWHERE_OUT_COUNTRY = "ONsseOxElW9";

const defenceUnits = fromPairs(defenceSites.map((ou) => [ou.id, ou.name]));

const fields = [
  "hDdStedsrHN",
  "NI0QRzJvQ0k",
  "identifier",
  "hDdStedsrHN",
  "FZzQbW8AWVd",
  "Yp1F4txx8tm",
  "bbnyNYD1wgS",
  "rpkH9ZPGJcX",
  "lySxMCMSo8Z",
  "ObwW38YrQHu",
  "X7tI86pr1y0",
  "ONsseOxElW9",
  "OW3erclrDW8",
  "wwX1eEiYLGR",
  "taGJD9hkX0s",
  "muCgXjnCfnS",
  "YvnFn4IjKzx",
  "ciCR6BBvIT4",
  "event_execution_date",
  "name",
  "districtName",
];

const api = axios.create({
  baseURL: "https://services.dhis2.hispuganda.org/wal/",
});

const findDistrictAndFacility = (data) => {
  const where = data[DOSE_PLACE];
  if (where === "Outside the country") {
    return {
      facility: data[ELSEWHERE_OUT_COUNTRY_FACILITY],
      district: data[ELSEWHERE_OUT_COUNTRY],
    };
  }

  return {
    facility: data[ELSEWHERE_IN_COUNTRY_FACILITY],
    district: data[ELSEWHERE_IN_COUNTRY_DISTRICT],
  };
};

const getFacilities = async () => {
  console.log("Fetching units");
  console.log("Fetching first cursor");
  let {
    data: {
      body: { rows, columns, cursor: currentCursor },
    },
  } = await api.post("sql", {
    body: {
      fetch_size: 1000,
      query: "select * from facilities",
    },
  });

  let allRows = rows.map((r) => {
    return fromPairs(r.map((x, i) => [columns[i].name, x]));
  });

  if (currentCursor) {
    do {
      console.log("Fetching next cursor");
      let {
        data: {
          body: { rows, cursor },
        },
      } = await api.post("sql", { body: { cursor: currentCursor } });
      allRows = allRows.concat(
        rows.map((r) => {
          return fromPairs(r.map((x, i) => [columns[i].name, x]));
        })
      );
      currentCursor = cursor;
    } while (!!currentCursor);
  }

  return fromPairs(
    allRows.map(({ id, ...rest }) => {
      return [id, { id, ...rest }];
    })
  );
};

const processCertificate = (previous, foundFacilities) => {
  const allTies = uniq(
    previous.map((record) => String(record.tei_uid).toLowerCase())
  );
  const updated = previous.map((p) => {
    const facility = foundFacilities[p.orgunit] || {};
    p = {
      ...p,
      ...facility,
      identifier: p[NIN_ATTRIBUTE] || p[OTHER_ID],
      matched: allTies.join("").toLowerCase(),
    };
    const siteChange = defenceUnits[p.orgunit];
    if (siteChange) {
      p = {
        ...p,
        name: siteChange,
        orgUnitName: siteChange,
      };
    }
    if (p.views) {
      p = {
        ...p,
        views: p.views + 1,
      };
    } else {
      p = { ...p, views: 1 };
    }

    const { facility: fac, district } = findDistrictAndFacility(p);

    return { ...p, facility: fac, district };
  });

  const doses = groupBy(updated, "LUIsbsm3okG");
  const { BOOSTER, ...others } = doses;

  let availableDoses = {};
  if (BOOSTER) {
    orderBy(
      mergeByKey("event_execution_date", BOOSTER),
      ["event_execution_date"],
      ["desc"]
    )
      .reverse()
      .forEach((d, i) => {
        const all = Object.entries(pick(d, fields)).map(([key, value]) => [
          `BOOSTER${i + 1}${key}`,
          value,
        ]);
        availableDoses = {
          ...availableDoses,
          ...fromPairs(all),
        };
      });
  }
  Object.entries(others).forEach(([dose, allDoses]) => {
    const gotDoses = mergeByKey("LUIsbsm3okG", allDoses);
    const all = Object.entries(pick(gotDoses[0], fields)).map(
      ([key, value]) => [`${dose}${key}`, value]
    );
    availableDoses = {
      ...availableDoses,
      ...fromPairs(all),
    };
  });
  return availableDoses;
};

const queryCertificate = async (v, facilities) => {
  let must = [];
  if (v[0] !== null && v[1] !== null) {
    must = [
      {
        term: {
          "Ewi7FUfcHAD.keyword": v[0],
        },
      },
      {
        term: {
          "YvnFn4IjKzx.keyword": v[1],
        },
      },
      {
        exists: {
          field: "bbnyNYD1wgS",
        },
      },
      {
        exists: {
          field: "LUIsbsm3okG",
        },
      },
    ];
  } else if (v[0] !== null) {
    return [
      {
        term: {
          "Ewi7FUfcHAD.keyword": v[0],
        },
      },
      {
        exists: {
          field: "bbnyNYD1wgS",
        },
      },
      {
        exists: {
          field: "LUIsbsm3okG",
        },
      },
    ];
  } else if (v[1] !== null) {
    must = [
      {
        term: {
          "YvnFn4IjKzx.keyword": v[1],
        },
      },
      {
        exists: {
          field: "bbnyNYD1wgS",
        },
      },
      {
        exists: {
          field: "LUIsbsm3okG",
        },
      },
    ];
  }

  if (must.length > 0) {
    const {
      data: {
        hits: { hits },
      },
    } = await api.post("search", {
      index: "epivac",
      query: {
        bool: { must },
      },
    });

    return processCertificate(
      hits.map(({ _source }) => _source),
      facilities
    );
  }

  return null;
};

const download = async () => {
  // const facilities = await getFacilities();
  let must = [
    {
      match: {
        event_deleted: false,
      },
    },
    {
      match: {
        tei_deleted: false,
      },
    },
    {
      match: {
        pi_deleted: false,
      },
    },
    {
      exists: {
        field: "bbnyNYD1wgS",
      },
    },
    {
      exists: {
        field: "LUIsbsm3okG",
      },
    },

    {
      bool: {
        should: [
          {
            exists: {
              field: "Ewi7FUfcHAD",
            },
          },
          {
            exists: {
              field: "YvnFn4IjKzx",
            },
          },
        ],
      },
    },
  ];

  let {
    data: {
      body: { rows, columns, cursor: currentCursor },
    },
  } = await api.post("sql", {
    body: {
      fetch_size: 10,
      query:
        "select hDdStedsrHN as VaccinationCardNo,pCnbIVhxv4j as ClientCategory,Ewi7FUfcHAD as NIN,ud4YNaOH3Dw as AlternativeID,YvnFn4IjKzx as AlternativeIDNo,sB1IHYu2xQT as ClientName,FZzQbW8AWVd as Sex,NI0QRzJvQ0k as DateOfBirth,s2Fmb8zgEem as Age,ciCR6BBvIT4 as TelephoneContact,SSGgoQ6SnCx as AlternativeTelephoneContact,Sqq2zIYWBOK as RelationshipWithAlternativeContact,Za0xkyQDpxA as DistrictAndSubcounty,M3trOwAtMqR as Parish,zyhxsh0kFx5 as Village,LY2bDXpNvS7 as Occupation,CFbojfdkIIj as PriorityPopulationGroup,CVv0CLvLc2i as OtherGroups,ZHF7EsKgiaM as PlaceOfWork,pK0K4T2Cq2f as MainOccupation,ZpvNoELGUnJ as InstitutionLevel from epivac group by hDdStedsrHN,pCnbIVhxv4j,Ewi7FUfcHAD,ud4YNaOH3Dw ,YvnFn4IjKzx,sB1IHYu2xQT,FZzQbW8AWVd,NI0QRzJvQ0k,s2Fmb8zgEem,ciCR6BBvIT4,SSGgoQ6SnCx,Sqq2zIYWBOK,Za0xkyQDpxA,M3trOwAtMqR,zyhxsh0kFx5,LY2bDXpNvS7,CFbojfdkIIj,CVv0CLvLc2i,ZHF7EsKgiaM,pK0K4T2Cq2f,ZpvNoELGUnJ",
      filter: { bool: { must } },
    },
  });

  const processed = rows.map((row) =>
    fromPairs(row.map((r, i) => [columns[i].name, r]))
  );
  console.log(processed);
  // let allRows = [];
  // let ch = 1;
  // for (const row of rows) {
  //   const data = await queryCertificate(row, facilities);
  //   if (data) {
  //     allRows = [...allRows, data];
  //   }
  // }

  // const csv = await parser.parse(allRows).promise();
  // writeFileSync(`chunk${ch}.csv`, csv);
  // ch = ch + 1;

  // if (currentCursor) {
  //   do {
  //     console.log("Fetching next cursor");
  //     let {
  //       data: {
  //         body: { rows, cursor },
  //       },
  //     } = await api.post("sql", { body: { cursor: currentCursor } });
  //     let allRows = [];
  //     for (const row of rows) {
  //       const data = await queryCertificate(row, facilities);
  //       if (data) {
  //         allRows = [...allRows, data];
  //       }
  //     }
  //     const csv = await parser.parse(allRows).promise();
  //     writeFileSync(`chunk${ch}.csv`, csv);
  //     ch = ch + 1;
  //     currentCursor = cursor;
  //   } while (!!currentCursor);
  // }
};

download().then(() => console.log("Done"));
