/* global $ */
import "zx/globals";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const METADATA_FILE = path.resolve(__dirname, "metadata.json");
const metadata = JSON.parse(await fs.promises.readFile(METADATA_FILE, "utf8"));

const downloadsPage = await fetch("http://ergast.com/downloads/");
const downloadsPageText = await downloadsPage.text();
const $$ = cheerio.load(downloadsPageText);

const totalRows = $$("table > tbody > tr").length;
const csvRowIndex = Array.from({ length: totalRows }).findIndex((_, i) => {
  const text = $$(
    `table > tbody > tr:nth-child(${i}) > td:nth-child(2)`
  ).text();

  return text.trim() === "f1db_csv.zip";
});

const lastModified = $$(
  `table > tbody > tr:nth-child(${csvRowIndex}) > td:nth-child(3)`
)
  .text()
  .trim();

console.log("Last Modified:", lastModified);

if (!lastModified) {
  console.log("Could not find database update time!");
  process.exit(1);
}

if (
  !argv.includes("--force") &&
  metadata.databaseLastModified === lastModified
) {
  console.log("No new data found!");
  process.exit(0);
}

console.log("New data exists");
console.log("Downloading latest data");
await $`wget http://ergast.com/downloads/f1db_csv.zip -O f1db_csv.zip`;
await $`unzip -q -o f1db_csv.zip -d ./f1db`;

console.log("writing metadata");
const newContent = { databaseLastModified: lastModified };
await fs.promises.writeFile(
  METADATA_FILE,
  JSON.stringify(newContent, null, 2),
  "utf8"
);
console.log("done!");
