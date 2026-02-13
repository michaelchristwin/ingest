import { MeterClient } from "m3ter-graphql-client";

export const meterClient = new MeterClient({
  endpoint: "https://subgraph.m3ter.ing/v2",
});
