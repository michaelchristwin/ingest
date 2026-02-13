import { createPublicClient, http } from "viem";
import { sepolia } from "viem/chains";

const API_KEY = process.env.INFURA_API_KEY;
if (!API_KEY) {
  console.error("API key unavailable");
}
const RPC_URL = `https://sepolia.infura.io/v3/${API_KEY}`;

export const publicClient = createPublicClient({
  chain: sepolia,
  transport: http(RPC_URL),
});
