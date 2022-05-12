//@ts-check
import "dotenv/config";
import fetch from "cross-fetch";
import fs from "fs";
import merge from "lodash.merge";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);

const __dirname = path.dirname(__filename);

const auth_token = process.env.STADTPULS_AUTH_TOKEN_TEGEL;
const number_of_records_per_chunks = 500;
const store = JSON.parse(
  fs.readFileSync(path.join(__dirname, "store.json"), "utf8")
);
// Codeblock 2
// const store = {
//   "5fcf669dfab469001ce52232": {
//     "5fcf669dfab469001ce52239": {
//       stadtpuls_sensor_id: 91,
//       comment: "Beleuchtungsstärke",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52238": {
//       stadtpuls_sensor_id: null,
//       comment: "Luftdruck",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52237": {
//       stadtpuls_sensor_id: null,
//       comment: "Temperatur (mit BMP280? stimmt nicht)",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52236": {
//       stadtpuls_sensor_id: null,
//       comment: "Lautstärke",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52235": {
//       stadtpuls_sensor_id: null,
//       comment: "Temperatur (mit HDC1080? stimmt nicht)",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52234": {
//       stadtpuls_sensor_id: null,
//       comment: "rel. Luftfeuchte",
//       last_measurement: null,
//     },
//     "5fcf669dfab469001ce52233": {
//       stadtpuls_sensor_id: null,
//       comment: "UV-Intensität",
//       last_measurement: null,
//     },
//   },
// };

/**
 * @param {string | URL} url
 */
function get_ids_from_url(url) {
  const ids = new URL(url).pathname.split("/").filter((section) => {
    if (section !== "data" && section !== "boxes") {
      return section;
    }
  });
  return { box_id: ids[0], sensor_id: ids[1] };
}

/**
 * @param {string | number | Date} date
 * @param {number} milliseconds
 */
function add_milliseconds(date, milliseconds) {
  const result = new Date(date);
  result.setMilliseconds(result.getMilliseconds() + milliseconds);
  return result.toISOString();
}

async function main(store) {
  const box_ids = Object.keys(store);
  const urls = [];
  for (const id of box_ids) {
    for (const sensor_id of Object.keys(store[id])) {
      if (store[id][sensor_id].stadtpuls_sensor_id === null) {
        continue;
      }
      const data = {
        box_id: id,
        sensor_id,
        url: `https://api.opensensemap.org/boxes/${id}/data/${sensor_id}?from-date=${
          store[id][sensor_id].last_measurement
            ? add_milliseconds(
                store[id][sensor_id].last_measurement.createdAt,
                1
              )
            : "2000-01-01T00:00:00Z"
        }&download=false&format=json`,
        stadtpuls_sensor_id: store[id][sensor_id].stadtpuls_sensor_id,
      };
      urls.push(data);
    }
  }
  // setup and execute GET requests to opensensemap
  const requests = [];
  urls.forEach((url_item) => {
    requests.push(
      new Promise((resolve) => setTimeout(resolve, 1000)).then(() =>
        fetch(url_item.url)
      )
    );
  });
  const responses = await Promise.all(requests);
  const promises = responses
    .map(async (response) => {
      if (response.ok === true) {
        const json = await response.json();
        if (json.length === 0) {
          return null;
        }
        const ids = get_ids_from_url(response.url);
        return { json, ids };
      } else {
        return null;
      }
    })
    .filter((item) => item !== null);
  /**
   * @type {Array<{json: Array<{createdAt: string, value: string}>, ids: {box_id: string, sensor_id: string}}>}
   */
  const data = await Promise.all(promises).catch((e) => {
    console.error(e);
  });

  console.info(
    data
      .filter((item) => item !== null)
      .map(
        (datum) =>
          `Got ${datum.json.length} records from opensensemap on sensor: ${datum.ids.sensor_id}`
      )
      .join("\n")
  );
  //-----------------
  if (data.filter((item) => item !== null).length === 0) {
    console.info("No new data found - abort");
    return store;
  }
  // combine store data with responses from opensensemap
  const hash_map = data
    .filter((item) => item !== null)
    .reduce((result, curr) => {
      if (!result[curr.ids.box_id]) {
        result[curr.ids.box_id] = {};
      }
      result[curr.ids.box_id][curr.ids.sensor_id] = { data: curr.json };
      return result;
    }, {});
  const merged_has_map_store = merge(store, hash_map);
  delete merged_has_map_store.json;
  delete merged_has_map_store.ids;
  //--------

  // setup and execute POST requests to stadtpuls
  const post_requests = [];
  for (const id of box_ids) {
    const sensors = merged_has_map_store[id];
    for (const sensor_id of Object.keys(sensors)) {
      if (sensors[sensor_id].stadtpuls_sensor_id === null) {
        continue;
      }
      const last_measurement = sensors[sensor_id].data.reduce((a, b) => {
        return new Date(a.createdAt) > new Date(b.createdAt) ? a : b;
      });
      const all_records = sensors[sensor_id].data.map((record) => {
        return {
          measurements: [parseFloat(record.value)],
          recorded_at: record.createdAt,
        };
      });
      console.info(
        `Created ${all_records.length} records for stadtpuls sensor: ${sensors[sensor_id].stadtpuls_sensor_id}`
      );
      if (all_records.length === 0) {
        continue;
      }
      // update store to keep track of last_measurement
      store[id][sensor_id].last_measurement = last_measurement;

      const chunks = new Array(
        Math.ceil(all_records.length / number_of_records_per_chunks)
      )
        .fill()
        .map((_) => all_records.splice(0, number_of_records_per_chunks));
      //----------------------------------------------------------------

      console.info(
        `Created chunks:${chunks.length} for stadtpuls sensor: ${sensors[sensor_id].stadtpuls_sensor_id}`
      );
      const p = chunks.map(async (records) => {
        const url = `https://api.stadtpuls.com/api/v3/sensors/${sensors[sensor_id].stadtpuls_sensor_id}/records`;
        await new Promise((resolve) => setTimeout(resolve, 1000));
        return await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${auth_token}`,
          },
          body: JSON.stringify({ records }),
        });
      });
      post_requests.push(...p);
    }
  }
  const post_responses = await Promise.all(post_requests).catch((e) => {
    console.error(e);
  });
  post_responses.forEach((res) => {
    if (res.status === 201) {
      console.info(res.url);
      console.info(res.status);
    } else {
      console.error(res.url);
      console.error(res.status);
    }
  });

  // Housekeeping
  Object.keys(store).forEach((box_id) => {
    Object.keys(store[box_id]).forEach((sensor_id) => {
      delete store[box_id][sensor_id].data;
    });
  });
  return store;
}

main(store)
  .then((updated_store) => {
    fs.writeFile(
      path.join(__dirname, "store.json"),
      JSON.stringify(updated_store),
      "utf8",
      (err) => {
        if (err) {
          console.error(err);
        }
      }
    );
  })
  .catch(console.error);
