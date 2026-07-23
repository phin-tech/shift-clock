// A small SDK-instrumented workflow in TypeScript (run by Node's built-in type
// stripping: `node flows/report.ts`).

import { workflow, step, getParam, log } from "../sdk/typescript/shift_clock.mjs";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const fetchData = step(
  async (): Promise<{ items: number }> => {
    await sleep(150);
    return { items: 42 };
  },
  { name: "fetch", retries: 1 },
);

const render = step(
  async (data: { items: number }): Promise<{ ok: boolean }> => {
    await sleep(150);
    log(`rendered ${data.items} items`);
    return { ok: true };
  },
  { name: "render" },
);

await workflow(async () => {
  const format = getParam("format", "html");
  log(`report format = ${format}`);
  const data = await fetchData();
  await render(data);
});
