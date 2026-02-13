import { publicClient } from "../config/viem-public-client";
import { rollupContract } from "../config/rollup";
import { fromHex } from "viem";

async function getLatestNoncePerMeter(meterId: number) {
  const nonceBytes = await publicClient.readContract({
    ...rollupContract,
    functionName: "nonce",
    args: [BigInt(meterId)],
  });
  const nonce = fromHex(nonceBytes, "number");

  return nonce;
}

export { getLatestNoncePerMeter };
