import fs from "fs";
import path from "path";
import { MeterClient, MeterDataPointOrderBy } from "m3ter-graphql-client";
import { DuckDBConnection } from "@duckdb/node-api";
import { getLatestNoncePerMeter } from "./get-latest-nonce";
import { getStartNonce } from "../utils/queries/last-stored-nonce";
import { createArrayRange } from "../utils/array-range";

export async function ingestNewData(
  client: MeterClient,
  meterId: number,
  db: DuckDBConnection,
) {
  let cursor: string | undefined;

  const tmpDir = path.resolve("./tmp_new_jsonl");
  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir);

  let pageIndex = 0;
  const latestNonce = await getLatestNoncePerMeter(meterId);
  const startNonce = await getStartNonce(db, meterId);

  // 2️⃣ Pagination loop
  let dataPointEdges = await client.v2.dataPoints.getMeterDataPoints({
    meterNumber: meterId,
    first: 1000,
    nonces: createArrayRange(startNonce, latestNonce),
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
      nonces: createArrayRange(startNonce, latestNonce),
      sortBy: MeterDataPointOrderBy.HEIGHT_ASC,
    });

    pageIndex++;
  }

  console.log(`New data ingestion for meter ${meterId} complete!`);
}
