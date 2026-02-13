import { fetch } from "bun";
import { Result, ok, err } from "neverthrow";

type DataResponse = {
  height: number;
  timestamp: number;
};
const ARWEAVE_URL = "https://arweave.net";

async function getLatestArweaveBlock(): Promise<Result<DataResponse, string>> {
  const infoResp = await fetch(`${ARWEAVE_URL}/info`);
  if (!infoResp.ok) return err("Failed to fetch latest info.");
  const infoData = await infoResp.json();
  const blockResp = await fetch(
    `${ARWEAVE_URL}/block/height/${infoData.height}`,
  );
  if (!blockResp.ok) return err("Failed to fetch latest block");
  const blockData = await blockResp.json();
  const dataResp: DataResponse = {
    height: infoData.height,
    timestamp: blockData.timestamp,
  };
  return ok(dataResp);
}

export { getLatestArweaveBlock };
