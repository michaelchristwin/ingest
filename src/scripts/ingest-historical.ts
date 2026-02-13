import fs from "fs";
import path from "path";
import { MeterClient, MeterDataPointOrderBy } from "m3ter-graphql-client";
import { IdsCache } from "../utils/cache";
import { duckDbInstance } from "../config/duckdb";
import { newMetersPoll } from "./poll-for-new-meters";
import { meterClient } from "../config/meter-client";
import { DuckDBConnection } from "@duckdb/node-api";

export async function ingestHistoricalData(
  client: MeterClient,
  meterId: number,
  db: DuckDBConnection,
) {
  // 1️⃣ Create main table (once)
  await db.run(`
    CREATE TABLE IF NOT EXISTS meter_data_points (
      transaction_id VARCHAR PRIMARY KEY,
      meter_id INTEGER NOT NULL,
      ts_utc TIMESTAMP NOT NULL,
      ts_unix_ms BIGINT NOT NULL,
      nonce INTEGER NOT NULL,
      voltage DOUBLE NOT NULL,
      energy DOUBLE NOT NULL,
      signature VARCHAR NOT NULL
    );
  `);

  let cursor: string | undefined;

  const tmpDir = path.resolve("./tmp_historical_jsonl");
  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir);

  let pageIndex = 0;

  // 2️⃣ Pagination loop
  let dataPointEdges = await client.v2.dataPoints.getMeterDataPoints({
    meterNumber: meterId,
    first: 1000,
    sortBy: MeterDataPointOrderBy.HEIGHT_ASC,
  });

  while (dataPointEdges.length > 0) {
    // 3️⃣ Write JSONL for current page
    const jsonlFile = path.join(
      tmpDir,
      `meter_${meterId}_page_${pageIndex}.jsonl`,
    );
    const fileStream = fs.createWriteStream(jsonlFile);

    for (const edge of dataPointEdges) {
      const node = edge.node;
      if (!node) continue;
      const payload = node.payload;
      if (
        !payload?.nonce ||
        payload?.voltage == null ||
        payload?.energy == null ||
        !payload?.signature
      )
        continue;

      const record = {
        transaction_id: node.transactionId,
        meter_id: node.meterNumber,
        ts_unix_ms: node.timestamp,
        nonce: payload.nonce,
        voltage: payload.voltage,
        energy: payload.energy,
        signature: payload.signature,
      };

      fileStream.write(JSON.stringify(record) + "\n");
    }

    fileStream.end();
    await new Promise((res) => fileStream.on("finish", res));

    // 4️⃣ Load JSONL into DuckDB fast
    await db.run(`
      INSERT INTO meter_data_points
      SELECT
        transaction_id,
        meter_id,
        to_timestamp(ts_unix_ms / 1000.0) AS ts_utc,
        ts_unix_ms,
        nonce,
        voltage,
        energy,
        signature
      FROM read_json_auto('${jsonlFile}')
      ON CONFLICT(transaction_id) DO NOTHING;
    `);
    fs.unlinkSync(jsonlFile);
    // 5️⃣ Prepare next page
    cursor = dataPointEdges[dataPointEdges.length - 1]?.cursor;
    if (!cursor || dataPointEdges.length < 1000) break;

    dataPointEdges = await client.v2.dataPoints.getMeterDataPoints({
      meterNumber: meterId,
      after: cursor,
      first: 1000,
      sortBy: MeterDataPointOrderBy.HEIGHT_ASC,
    });

    pageIndex++;
  }

  console.log(`Historical data ingestion for meter ID ${meterId} complete!`);
}

const ingestall = async () => {
  let meterIds = IdsCache.get("meter_ids");
  if (!meterIds) {
    meterIds = await newMetersPoll();
  }
  const db = await duckDbInstance.connect();
  await db.run(`
      CREATE TABLE IF NOT EXISTS ingestion_state (
        key VARCHAR PRIMARY KEY,
        value VARCHAR NOT NULL
      );
    `);
  const state = await db.run(`
   SELECT value
   FROM ingestion_state
   WHERE key = 'historical_ingest_done'
   LIMIT 1;
 `);

  const rows = await state.getRows();

  if (rows.length > 0 && rows[0][0] === "true") {
    console.log("Historical ingest already done, exiting...");
    return;
  }
  for (const id of meterIds) {
    await ingestHistoricalData(meterClient, id, db);
  }

  await db.run(`
    INSERT INTO ingestion_state (key, value)
    VALUES ('historical_ingest_done', 'true')
    ON CONFLICT(key) DO UPDATE SET value = 'true';
  `);
  db.closeSync();
};

ingestall();
