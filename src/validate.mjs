// src/validate.mjs
import fs from "node:fs";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
import chalkTable from "chalk-table";
import chalk from "chalk";

dotenv.config();

function normalizeProjectKey(input) {
	return input
		.toString()
		.trim()
		.toUpperCase()
		.replace(/[^A-Z0-9]+/g, "_")
		.replace(/^_+|_+$/g, "");
}

function normalizeEnvName(input) {
	return input
		.toString()
		.trim()
		.toUpperCase()
		.replace(/[^A-Z0-9]+/g, "_")
		.replace(/^_+|_+$/g, "");
}

function normalizeValue(v) {
	return (v ?? "").toString().trim();
}

function requireEnv(name, hint) {
	const v = normalizeValue(process.env[name]);
	if (!v) {
		throw new Error(
			`Variável obrigatória ausente no .env: ${name}${hint ? ` (${hint})` : ""}`
		);
	}
	return v;
}

function requireFile(filePath, hint) {
	if (!fs.existsSync(filePath)) {
		throw new Error(
			`Arquivo não encontrado: ${filePath}${hint ? ` (${hint})` : ""}`
		);
	}
	return fs.readFileSync(filePath, "utf8");
}

async function fetchText(url, { headers = {} } = {}) {
	const res = await fetch(url, { headers });
	if (!res.ok) {
		const body = await res.text().catch(() => "");
		throw new Error(`HTTP ${res.status} ao baixar ${url}\n${body}`);
	}
	return res.text();
}

function basicAuthHeaderFromPat(pat) {
	const token = Buffer.from(`:${pat}`, "utf8").toString("base64");
	return `Basic ${token}`;
}

function buildGoogleCsvUrl({ spreadsheetId, gid }) {
	const safeGid = gid === undefined || gid === null || gid === "" ? "0" : gid;
	return `https://docs.google.com/spreadsheets/d/${encodeURIComponent(
		spreadsheetId
	)}/export?format=csv&gid=${encodeURIComponent(safeGid)}`;
}

async function downloadGoogleCsvText({ projectKey }) {
	const spreadsheetId = requireEnv(
		`${projectKey}_GOOGLE_SPREADSHEET_ID`,
		"ex: 11nuS_..."
	);
	const gid = normalizeValue(process.env[`${projectKey}_GOOGLE_GID`]) || "0";

	const url = buildGoogleCsvUrl({ spreadsheetId, gid });
	return fetchText(url);
}

async function downloadAzureVariableGroupJson({ projectKey }) {
	const org = requireEnv("AZURE_ORG");
	const project = requireEnv("AZURE_PROJECT");
	const pat = requireEnv("AZURE_PAT");
	const groupId = requireEnv(`${projectKey}_AZURE_GROUP_ID`);

	const apiUrl = `https://dev.azure.com/${encodeURIComponent(
		org
	)}/${encodeURIComponent(
		project
	)}/_apis/distributedtask/variablegroups/${encodeURIComponent(
		groupId
	)}?api-version=7.1-preview.2`;

	const auth = basicAuthHeaderFromPat(pat);
	const jsonText = await fetchText(apiUrl, {
		headers: {
			Authorization: auth,
			Accept: "application/json",
		},
	});

	return JSON.parse(jsonText);
}

async function downloadAzureConfigMapYaml({ projectKey, overrideRef }) {
	const org = requireEnv("AZURE_ORG");
	const project = requireEnv("AZURE_PROJECT");
	const pat = requireEnv("AZURE_PAT");

	const repo = requireEnv(`${projectKey}_AZURE_REPO`);
	const ref = normalizeValue(overrideRef) || normalizeValue(process.env[`${projectKey}_CONFIGMAP_REF`]) || "main";
	const filePath =
		normalizeValue(process.env[`${projectKey}_CONFIGMAP_PATH`]) ||
		"/manifests/configmap.yml";

	const apiUrl =
		`https://dev.azure.com/${encodeURIComponent(
			org
		)}/${encodeURIComponent(project)}` +
		`/_apis/git/repositories/${encodeURIComponent(repo)}/items` +
		`?path=${encodeURIComponent(filePath)}` +
		`&includeContent=true` +
		`&versionDescriptor.version=${encodeURIComponent(ref)}` +
		`&api-version=7.1`;

	const auth = basicAuthHeaderFromPat(pat);
	const jsonText = await fetchText(apiUrl, {
		headers: {
			Authorization: auth,
			Accept: "application/json",
		},
	});

	const data = JSON.parse(jsonText);
	const content = data?.content;
	if (!content) {
		throw new Error(
			`ConfigMap vazio/ausente. Verifique repo/ref/path.\nrepo=${repo}\nref=${ref}\npath=${filePath}`
		);
	}

	return content;
}

function parseCsvText(text) {
	return parse(text, {
		columns: true,
		skip_empty_lines: true,
		bom: true,
		relax_quotes: true,
		relax_column_count: true,
		trim: true,
	});
}

function pickFirstColumnName(rows) {
	if (!rows.length) throw new Error("CSV do Google está vazio (sem linhas).");
	const cols = Object.keys(rows[0] ?? {});
	if (!cols.length) throw new Error("CSV do Google não tem colunas (header ausente?).");
	return cols[0];
}

function isSectionMarker(key) {
	return /^#[^#]+#$/.test(key);
}

function parseSectionName(marker) {
	return marker.replace(/^#/, "").replace(/#$/, "").trim().toUpperCase();
}

function buildAzureMapFromVariableGroupJson(variableGroupJson) {
	const vars = variableGroupJson?.variables ?? {};
	const map = new Map();
	for (const [k, v] of Object.entries(vars)) {
		const isSecret = v?.isSecret === true;
		const value = isSecret ? "" : normalizeValue(v?.value);
		map.set(k, value);
	}
	return map;
}

function buildGoogleItemsBySection({ googleRows, envName }) {
	const keyCol = pickFirstColumnName(googleRows);
	const envColWanted = normalizeEnvName(envName);

	const headerCols = googleRows[0] ? Object.keys(googleRows[0]) : [];
	const envCol = headerCols.find((c) => normalizeEnvName(c) === envColWanted) ?? null;
	const globalCol = headerCols.find((c) => normalizeEnvName(c) === "GLOBAL") ?? null;

	if (!envCol) {
		throw new Error(
			`Coluna do ambiente não encontrada no Google CSV: "${envColWanted}". Colunas disponíveis: ${headerCols.join(
				", "
			)}`
		);
	}
	if (!globalCol) {
		throw new Error(
			`Coluna GLOBAL não encontrada no Google CSV. Colunas disponíveis: ${headerCols.join(
				", "
			)}`
		);
	}

	const sections = new Map();
	const ensureSection = (name) => {
		if (!sections.has(name)) sections.set(name, []);
		return sections.get(name);
	};

	let currentSection = "APPLICATION_KEYS";
	ensureSection(currentSection);

	for (const row of googleRows) {
		const baseKey = normalizeValue(row[keyCol]);
		if (!baseKey) continue;

		if (isSectionMarker(baseKey)) {
			currentSection = parseSectionName(baseKey);
			ensureSection(currentSection);
			continue;
		}

		const envValue = normalizeValue(row[envCol]);
		const globalValue = normalizeValue(row[globalCol]);

		let expectedValue = envValue;
		let prefix = envColWanted;
		if (!expectedValue) {
			expectedValue = globalValue;
			prefix = "GLOBAL";
		}

		ensureSection(currentSection).push({
			section: currentSection,
			baseKey,
			prefix,
			expectedValue,
		});
	}

	return { sections };
}

function buildConfigMapTemplateIndex(configmapYamlText) {
	const byKey = new Map();
	const lines = configmapYamlText.split(/\r?\n/);

	let inData = false;
	let dataIndent = null;

	for (const rawLine of lines) {
		if (!rawLine.trim()) continue;

		if (!inData && rawLine.match(/^\s*data:\s*$/)) {
			inData = true;
			dataIndent = rawLine.match(/^(\s*)data:/)?.[1]?.length ?? 0;
			continue;
		}
		if (!inData) continue;

		const indent = rawLine.match(/^(\s*)/)?.[1]?.length ?? 0;
		if (dataIndent !== null && indent <= dataIndent) break;

		const m = rawLine.match(/^\s*([A-Za-z0-9_]+)\s*:\s*(.+)\s*$/);
		if (!m) continue;

		const key = m[1];
		let value = m[2].trim();

		if (
			(value.startsWith('"') && value.endsWith('"')) ||
			(value.startsWith("'") && value.endsWith("'"))
		) {
			value = value.slice(1, -1);
		}

		byKey.set(key, value);
	}

	return { byKey };
}

function expectedConfigmapTemplateForRow({ baseKey, prefix }) {
	if (normalizeEnvName(prefix) === "GLOBAL") return `#{${baseKey}}#`;
	return `#{$(ENV)${baseKey}}#`;
}

// status curto: ok | miss | tmpl
function configmapStatusForRow({ configIndex, baseKey, prefix }) {
	if (!configIndex) return "";
	const actual = configIndex.byKey.get(baseKey);
	if (!actual) return "miss";

	const expected = expectedConfigmapTemplateForRow({ baseKey, prefix });
	if (actual === expected) return "ok";
	return "tmpl";
}

function colorStatus(status) {
	switch (status) {
		case "missing-azure":
			return chalk.red(status);
		case "changed":
			return chalk.yellow(status);
		case "ok":
			return chalk.green(status);
		default:
			return status;
	}
}

function colorConfigmapStatus(status) {
	switch (status) {
		case "ok":
			return chalk.green(status);
		case "miss":
			return chalk.red(status);
		case "tmpl":
			return chalk.yellow(status);
		default:
			return status;
	}
}

function sortRowsGlobalFirstThenKey(rows) {
	const prefixRank = (p) => (normalizeEnvName(p) === "GLOBAL" ? 0 : 1);

	return [...rows].sort((a, b) => {
		const ra = prefixRank(a.prefix);
		const rb = prefixRank(b.prefix);
		if (ra !== rb) return ra - rb;

		const ka = normalizeValue(a.keyUsed).toUpperCase();
		const kb = normalizeValue(b.keyUsed).toUpperCase();
		return ka.localeCompare(kb);
	});
}

function diffSection({ sectionItems, azureMap, envName, configIndex }) {
	const env = normalizeEnvName(envName);
	const rows = [];

	for (const g of sectionItems) {
		let keyUsed = "";
		let actualValue;

		if (g.prefix === "GLOBAL") {
			keyUsed = g.baseKey;
			actualValue = azureMap.get(g.baseKey);
		} else {
			keyUsed = `${env}_${g.baseKey}`;
			actualValue = azureMap.get(keyUsed);
		}

		const configmap = configmapStatusForRow({
			configIndex,
			baseKey: g.baseKey,
			prefix: g.prefix,
		});

		if (actualValue === undefined) {
			rows.push({
				status: "missing-azure",
				configmap,
				keyUsed,
				prefix: g.prefix,
				expected: g.expectedValue,
				actual: "",
			});
			continue;
		}

		const expectedNorm = normalizeValue(g.expectedValue);
		const actualNorm = normalizeValue(actualValue);
		const status = expectedNorm !== actualNorm ? "changed" : "ok";

		rows.push({
			status,
			configmap,
			keyUsed,
			prefix: g.prefix,
			expected: g.expectedValue,
			actual: actualValue,
		});
	}

	return rows;
}

function printDiffTableApplicationKeys(rows, envLabel) {
	const sorted = sortRowsGlobalFirstThenKey(rows);

	const coloredRows = sorted.map((r) => ({
		...r,
		status: colorStatus(r.status),
		configmap: r.configmap ? colorConfigmapStatus(r.configmap) : "",
	}));

	console.log(
		chalkTable(
			{
				columns: [
					{ field: "status", name: "status" },
					{ field: "configmap", name: "configmap" },
					{ field: "keyUsed", name: "keyUsed" },
					{ field: "prefix", name: "prefix" },
					{ field: "expected", name: "expected (google)" },
					{ field: "actual", name: `actual (library - ${envLabel})` },
				],
			},
			coloredRows
		)
	);
}

function printDiffTableOtherSections(rows, envLabel) {
	const sorted = sortRowsGlobalFirstThenKey(rows);

	const coloredRows = sorted.map((r) => ({
		...r,
		status: colorStatus(r.status),
	}));

	console.log(
		chalkTable(
			{
				columns: [
					{ field: "status", name: "status" },
					{ field: "keyUsed", name: "keyUsed" },
					{ field: "prefix", name: "prefix" },
					{ field: "expected", name: "expected (google)" },
					{ field: "actual", name: `actual (library - ${envLabel})` },
				],
			},
			coloredRows
		)
	);
}

function printUsefulLinks({ project, projectKey }) {
	const azureOrg = normalizeValue(process.env.AZURE_ORG);
	const azureProject = normalizeValue(process.env.AZURE_PROJECT);
	const variableGroupId = normalizeValue(process.env[`${projectKey}_AZURE_GROUP_ID`]);
	const spreadsheetId = normalizeValue(process.env[`${projectKey}_GOOGLE_SPREADSHEET_ID`]);

	console.log("\n== links úteis ==");
	if (azureOrg && azureProject && variableGroupId) {
		console.log(
			`Azure Library (Variable Group): https://dev.azure.com/${encodeURIComponent(
				azureOrg
			)}/${encodeURIComponent(
				azureProject
			)}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId=${encodeURIComponent(
				variableGroupId
			)}`
		);
	}
	if (spreadsheetId) {
		console.log(
			`Google Sheet: https://docs.google.com/spreadsheets/d/${encodeURIComponent(
				spreadsheetId
			)}/edit`
		);
	}
}

export function registerValidate() {
	return {
		command: "validate <project> <env>",
		describe:
			"Valida as variáveis da Library e ConfigMap com base no Google Sheet",
		builder: (y) =>
			y
				.positional("project", {
					type: "string",
					describe: "Projeto (ex: subscription, gateway2).",
				})
				.positional("env", {
					type: "string",
					describe: "Ambiente (ex: hml2, prd).",
				})
				.option("configmap-branch", {
					type: "string",
					describe: "Branch/ref do ConfigMap no Azure Repos (ex: main).",
					default: "main",
				})
				.option("configmap-path", {
					type: "string",
					describe:
						"Caminho local para o configmap.yml. Se informado, NÃO baixa do repo. Use 'null' para desabilitar validação de configmap.",
				}),
		handler: async (argv) => {
			const project = argv.project.toString().trim();
			const envName = argv.env.toString().trim();
			const envLabel = normalizeEnvName(envName);
			const projectKey = normalizeProjectKey(project);

			const configmapBranch = normalizeValue(argv["configmap-branch"]);
			const configmapPathArg = normalizeValue(argv["configmap-path"]);

			const configmapDisabled =
				configmapPathArg.toLowerCase() === "null" ||
				configmapPathArg.toLowerCase() === "false" ||
				configmapPathArg.toLowerCase() === "off";

			const configmapFromFile =
				!configmapDisabled && configmapPathArg && configmapPathArg !== "-";

			// 1) baixar tudo em memória (configmap é opcional)
			const [googleCsvText, variableGroupJson] = await Promise.all([
				downloadGoogleCsvText({ projectKey }),
				downloadAzureVariableGroupJson({ projectKey }),
			]);

			let configIndex = null;
			if (!configmapDisabled) {
				let configmapYaml = "";

				if (configmapFromFile) {
					configmapYaml = requireFile(
						configmapPathArg,
						"passe um caminho válido em --configmap-path"
					);
				} else {
					configmapYaml = await downloadAzureConfigMapYaml({
						projectKey,
						overrideRef: configmapBranch,
					});
				}

				configIndex = buildConfigMapTemplateIndex(configmapYaml);
			}

			// 2) parse / indexes
			const googleRows = parseCsvText(googleCsvText);
			const azureMap = buildAzureMapFromVariableGroupJson(variableGroupJson);

			const googleData = buildGoogleItemsBySection({ googleRows, envName });

			// 3) imprimir por seção (mesma regra do diff atual)
			for (const [sectionName, sectionItems] of googleData.sections.entries()) {
				if (!sectionItems.length) continue;

				// ocultar temporariamente SECRETS
				if (normalizeEnvName(sectionName) === "SECRETS") continue;

				console.log(`\n== section: ${chalk.cyan(sectionName)} ==`);

				const isApplicationKeys =
					normalizeEnvName(sectionName) === "APPLICATION_KEYS";

				const rows = diffSection({
					sectionItems,
					azureMap,
					envName,
					configIndex: isApplicationKeys ? configIndex : null,
				});

				if (isApplicationKeys) {
					printDiffTableApplicationKeys(rows, envLabel);
				} else {
					printDiffTableOtherSections(rows, envLabel);
				}
			}

			printUsefulLinks({ project, projectKey });
		},
	};
}