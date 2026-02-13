import { DuckDBConnection } from "@duckdb/node-api";

export async function getStartNonce(db: DuckDBConnection, meterId: number) {
  const result = await db.run(
    `
    SELECT nonce
    FROM meter_data_points
    WHERE meter_id = ?
    ORDER BY ts_unix_ms DESC
    LIMIT 1
    `,
    [meterId],
  );

  const rows = await result.getRows();
  if (rows.length === 0) return 0;

  return rows[0][0] as number;
}
