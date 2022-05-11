import fetch from "cross-fetch";
import fs from "fs/promises";
import merge from "lodash.merge";
const auth_token = "";
const store = {
  "5fcf669dfab469001ce52232": {
    "5fcf669dfab469001ce52239": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52238": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52237": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52236": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52235": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52234": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
    "5fcf669dfab469001ce52233": {
      stadtpuls_sensor_id: null,
      comment: "something to remember which one is which",
      last_measurement: null,
    },
  },
};

function get_ids_from_url(url) {
  const ids = new URL(url).pathname.split("/").filter((section) => {
    if (section !== "data" && section !== "boxes") {
      return section;
    }
  });
  return { box_id: ids[0], sensor_id: ids[1] };
}

async function main(store) {
  const box_ids = Object.keys(store);
  const urls = [];
  for (const id of box_ids) {
    for (const sensor_id of Object.keys(store[id])) {
      const data = {
        box_id: id,
        sensor_id: sensor_id,
        url: `https://api.opensensemap.org/boxes/${id}/data/${sensor_id}?from-date=${
          store[id][sensor_id].last_measurement
            ? store[id][sensor_id].last_measurement.createdAt
            : "2000-01-01T00:00:00Z"
        }&download=false&format=json`,
        stadtpuls_sensor_id: store[id][sensor_id].stadtpuls_sensor_id,
      };
      urls.push(data);
    }
  }

  const requests = [];
  for (const url_item of urls) {
    requests.push(fetch(url_item.url));
  }
  const responses = await Promise.all(requests);
  const promises = responses.map(async (response) => {
    if (response.ok === true) {
      const json = await response.json();
      const ids = get_ids_from_url(response.url);
      return { json, ids };
    }
  });
  const data = await Promise.all(promises).catch((e) => {
    console.error(e);
  });

  const hash_map = data.reduce((result, curr) => {
    if (!result[curr.ids.box_id]) {
      result[curr.ids.box_id] = {};
    }
    result[curr.ids.box_id][curr.ids.sensor_id] = { data: curr.json };
    return result;
  }, {});
  const merged_has_map_store = merge(store, hash_map);
  delete merged_has_map_store.json;
  delete merged_has_map_store.ids;
  // merged_has_map_store; //?
  // await fs.writeFile("aggregate.json", JSON.stringify(merged_has_map_store));
  const post_requests = [];
  for (const id of box_ids) {
    const sensors = merged_has_map_store[id];
    for (const sensor_id of Object.keys(sensors)) {
      const last_measurement = sensors[sensor_id].data.reduce((a, b) => {
        return new Date(a.createdAt) > new Date(b.createdAt) ? a : b;
      });
      const all_records = sensors[sensor_id].data.map((record) => {
        return {
          measurement: parseFloat(record.value),
          created_at: record.createdAt,
        };
      });
      // write to the store of pipedream
      store[id][sensor_id].last_measurement = last_measurement;
      const n = 10;
      const chunks = new Array(Math.ceil(all_records.length / n))
        .fill()
        .map((_) => all_records.splice(0, n));

      const p = chunks.map(async (records) =>
        fetch(
          `http://api.stadtpuls.com/api/v3/sensors/${sensors[sensor_id].stadtpuls_sensor_id}/records`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${auth_token}`,
            },
            body: JSON.stringify({ records }),
          }
        )
      );
      post_requests.push(...p);
    }
  }
  const reponses = await Promise.all(post_requests).catch((e) => {
    console.log(e);
  });
  const post_responses = reponses.map(async (response) => {
    if (response.ok === true) {
      console.log(response.status);
      return true;
    } else {
      console.warn(response.status);
    }
  });
  await Promise.all(post_responses).catch((e) => {
    console.log(e);
  });

  return store;
}

main(store).catch(console.error);
