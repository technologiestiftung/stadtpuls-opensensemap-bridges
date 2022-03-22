// https://docs.opensensemap.org/
// https://opensensemap.org/explore/5e9af8d545f937001ce58076

import "dotenv/config";
// @ts-check
import fetch from "cross-fetch";
const senseBoxId = "5e9af8d545f937001ce58076";
// const STADTPULS_TOKEN = ""; // included via dotenv
const stadtpulsSensorId = 79;
async function main() {
  try {
    const osmResponse =
      await fetch(`https://api.opensensemap.org/boxes/${senseBoxId}?format=json
    `);

    let osmPayload;
    if (osmResponse.ok) {
      osmPayload = await osmResponse.json();
    } else {
      throw new Error("Could not get data from opensensemap");
    }
    const valueTypes = osmPayload.sensors.map((sensor) => sensor.title); //?
    const measurements = osmPayload.sensors
      .map((sensor) => parseFloat(sensor.lastMeasurement.value))
      .filter((measurement) => !isNaN(measurement)); //?
    console.log(
      "Got the following measurements",
      measurements,
      "from opensensemap sensebox",
      senseBoxId
    );

    const stadtpulsResponse = await fetch(
      `https://api.stadtpuls.com/api/v3/sensors/${stadtpulsSensorId}/records/`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.STADTPULS_TOKEN}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ measurements }),
      }
    );
    if (stadtpulsResponse.ok) {
      const stadtpulsJson = await stadtpulsResponse.json();
      console.log("POSTed data to stadtpuls", stadtpulsJson);
    } else {
      console.error(await stadtpulsResponse.text());
      throw new Error("Could not POST data to stadtpuls");
    }
  } catch (error) {
    throw error;
  }
}

let count = 0;
const interval = setInterval(() => {
  main().catch(console.error);
  count++;
  if (count === 5) {
    clearInterval(interval);
  }
}, 10000);
