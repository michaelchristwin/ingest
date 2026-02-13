import { Elysia } from "elysia";
import logixlysia from "logixlysia";
import { DuckDBConnection } from "@duckdb/node-api";
import { logConfig } from "./config/logger";
import { meterClient } from "./config/meter-client";
import { duckDbInstance } from "./config/duckdb";
import { newMetersPoll } from "./scripts/poll-for-new-meters";
import { err, Result } from "neverthrow";
import { ingestNewData } from "./scripts/ingest-new-data";

let db: DuckDBConnection;
const app = new Elysia()
  .onStart(async () => {
    db = await duckDbInstance.connect();
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

    await db.run(`
        CREATE TABLE IF NOT EXISTS ingestion_state (
          key VARCHAR PRIMARY KEY,
          value VARCHAR NOT NULL
        );
      `);
  })

  .use(
    logixlysia({
      config: logConfig,
    }),
  )
  .get("/", async ({ set }) => {
    try {
      const dataPointEdges = await meterClient.v2.meters.getMeters();
      set.status = 200;

      return { Meters: dataPointEdges };
    } catch (err: any) {
      set.status = 500;
      return { Error: "Internal server error" };
    }
  })
  .onStop(() => {
    duckDbInstance.closeSync();
  });

pollData();
app.listen(3001);

async function pollData(): Promise<Result<void, string>> {
  while (true) {
    const meterIds = await newMetersPoll();
    if (!meterIds) return err("meterIds is undefined");

    for (const id of meterIds) {
      await ingestNewData(meterClient, id, db);
    }

    await Bun.sleep(60_000 * 30);
  }
}
