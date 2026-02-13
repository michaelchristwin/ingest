import { fetch } from "bun";
import { IdsCache } from "../utils/cache";

async function newMetersPoll() {
  const query = `
    query MeterDataPoints {
      meters {
        meterNumber
      }
    }
  `;

  const res = await fetch("https://subgraph.m3ter.ing/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });

  const json = await res.json();

  //@ts-expect-error "Type unknown"
  const meters: number[] = json.data.meters.map((item) => item.meterNumber);
  if (IdsCache.get("meter_ids") !== meters) {
    IdsCache.set(`meter_ids`, meters);
  }
  console.log("Meters: ", meters);
  return meters;
}

export { newMetersPoll };
