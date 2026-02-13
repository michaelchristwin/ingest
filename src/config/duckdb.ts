import { DuckDBInstance } from "@duckdb/node-api";
const dbPath = process.env.DUCKDB_PATH ?? "./meter_readings.db";
export const duckDbInstance = await DuckDBInstance.create(dbPath, {
  threads: "4",
});
