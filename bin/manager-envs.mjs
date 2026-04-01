#!/usr/bin/env node
import "dotenv/config";
import yargs from "yargs/yargs";
import { hideBin } from "yargs/helpers";

import { registerValidate } from "../src/validate.mjs";
import { registerUpdate } from "../src/update.mjs";

yargs(hideBin(process.argv))
  .scriptName("manager-envs")
  .command(registerValidate())
  .command(registerUpdate())
  .demandCommand(1)
  .strict()
  .help()
  .parse();

