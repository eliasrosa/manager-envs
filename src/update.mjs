// src/update.mjs
import fs from "node:fs";
import path from "node:path";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
import chalk from "chalk";

function findEnvFileUp(startDir) {
	let dir = startDir;
	while (true) {
		const candidate = path.join(dir, ".env");
		if (fs.existsSync(candidate)) return candidate;
		const parent = path.dirname(dir);
		if (parent === dir) return null;
		dir = parent;
	}
}

// Permite executar de qualquer pasta.
// 1) Se passar --env-file, usa ele.
// 2) Se não, procura um .env subindo a partir do cwd.
const envFileArg = process.argv.find((a) => a.startsWith("--env-file="))?.split("=")[1];
const envPath = envFileArg || findEnvFileUp(process.cwd());

dotenv.config(envPath ? { path: envPath } : undefined);

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
		.replace(/^_+|_+$/g, "_")
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

function indexConfigMapDataLines(configmapYamlText) {
	// Retorna um índice para alterar linhas do data: mantendo formato original.
	const lines = configmapYamlText.split(/\r?\n/);
	let inData = false;
	let dataIndent = null;
	const keyToLineIndex = new Map();

	for (let i = 0; i < lines.length; i++) {
		const rawLine = lines[i];
		if (!rawLine.trim()) continue;

		if (!inData && rawLine.match(/^\s*data:\s*$/)) {
			inData = true;
			dataIndent = rawLine.match(/^(\s*)data:/)?.[1]?.length ?? 0;
			continue;
		}
		if (!inData) continue;

		const indent = rawLine.match(/^(\s*)/)?.[1]?.length ?? 0;
		if (dataIndent !== null && indent <= dataIndent) break;

		const m = rawLine.match(/^(\s*)([A-Za-z0-9_]+)\s*:\s*(.*)\s*$/);
		if (!m) continue;

		const key = m[2];
		keyToLineIndex.set(key, i);
	}

	return { lines, keyToLineIndex };
}

function yamlQuoteIfNeeded(v) {
	// ConfigMap data: quase sempre é string. Para segurança, quote se tiver chars "complicados".
	const s = (v ?? "").toString();
	if (s === "") return '""';
	if (/^[A-Za-z0-9_./:-]+$/.test(s)) return s;
	const escaped = s.replace(/"/g, '\\"');
	return `"${escaped}"`;
}

function expectedConfigmapTemplateForRow({ baseKey, prefix }) {
	// Mantém o padrão do repo: no configmap.yml o valor é um template.
	// GLOBAL: "#{KEY}#"
	// env:    "#{$(ENV)KEY}#"
	if (normalizeEnvName(prefix) === "GLOBAL") return `#{${baseKey}}#`;
	return `#{$(ENV)${baseKey}}#`;
}

function buildDesiredKeyToValue({ googleData, envName }) {
	const env = normalizeEnvName(envName);
	const map = new Map();

	// O update deve afetar SOMENTE a seção APPLICATION_KEYS.
	const items =
		googleData.sections.get("APPLICATION_KEYS") ||
		googleData.sections.get("APPLICATION_KEYS".toUpperCase()) ||
		[];

	for (const g of items) {
		// No configmap, a chave é sempre baseKey.
		// Valor salvo no YAML deve ser template (ex: "#{$(ENV)API_GATEWAY_BASIC_CLIENT}#").
		const template = expectedConfigmapTemplateForRow({
			baseKey: g.baseKey,
			prefix: g.prefix,
		});
		map.set(g.baseKey, template);
	}

	return { env, map };
}

function rewriteConfigMapDataSection({ configmapYamlText, desired }) {
	// Apaga TODO o conteúdo do bloco `data:` e reescreve do zero conforme padrão.
	// Mantém o restante do YAML intacto.
	const lines = configmapYamlText.split(/\r?\n/);

	let dataLineIndex = -1;
	let dataIndent = 0;
	let dataEndIndex = -1;

	for (let i = 0; i < lines.length; i++) {
		if (lines[i].match(/^\s*data:\s*$/)) {
			dataLineIndex = i;
			dataIndent = lines[i].match(/^(\s*)data:/)?.[1]?.length ?? 0;
			break;
		}
	}
	if (dataLineIndex === -1) {
		throw new Error("Bloco 'data:' não encontrado no configmap.yml");
	}

	// encontra fim do bloco data (primeira linha com indent <= dataIndent)
	dataEndIndex = lines.length;
	for (let i = dataLineIndex + 1; i < lines.length; i++) {
		const raw = lines[i];
		if (!raw.trim()) continue;
		const indent = raw.match(/^(\s*)/)?.[1]?.length ?? 0;
		if (indent <= dataIndent) {
			dataEndIndex = i;
			break;
		}
	}

	const dataValueIndent = " ".repeat(dataIndent + 2);

	const newDataLines = [];
	const sortedEntries = [...desired.map.entries()]
		.map(([k, v]) => [String(k), v])
		.sort((a, b) => a[0].localeCompare(b[0]));

	for (const [key, wantedRaw] of sortedEntries) {
		const wantedValueYaml = yamlQuoteIfNeeded(wantedRaw);
		newDataLines.push(`${dataValueIndent}${key}: ${wantedValueYaml}`);
	}

	const before = lines.slice(0, dataLineIndex + 1);
	const after = lines.slice(dataEndIndex);
	const updatedLines = [...before, ...newDataLines, ...after];

	return { updatedText: updatedLines.join("\n") };
}

function applyConfigMapUpdates({ configmapYamlText, desired }) {
	// Mantém o retorno para log como está (lista de mudanças).
	// Para isso, compara data antigo vs novo (por chave), mas o write é sempre rebuild.
	const beforeIdx = indexConfigMapDataLines(configmapYamlText);
	const beforeMap = new Map();
	for (const [k, lineIndex] of beforeIdx.keyToLineIndex.entries()) {
		const line = beforeIdx.lines[lineIndex];
		const m = line.match(/^(\s*)([A-Za-z0-9_]+)\s*:\s*(.*)\s*$/);
		if (!m) continue;
		beforeMap.set(k, (m[3] ?? "").trim());
	}

	const { updatedText } = rewriteConfigMapDataSection({ configmapYamlText, desired });

	const afterIdx = indexConfigMapDataLines(updatedText);
	const afterMap = new Map();
	for (const [k, lineIndex] of afterIdx.keyToLineIndex.entries()) {
		const line = afterIdx.lines[lineIndex];
		const m = line.match(/^(\s*)([A-Za-z0-9_]+)\s*:\s*(.*)\s*$/);
		if (!m) continue;
		afterMap.set(k, (m[3] ?? "").trim());
	}

	const allKeys = new Set([...beforeMap.keys(), ...afterMap.keys()]);
	const changed = [];
	for (const key of [...allKeys].sort()) {
		const from = beforeMap.get(key) ?? "";
		const to = afterMap.get(key) ?? "";
		if (from !== to) changed.push({ key, from, to });
	}

	// Como agora a regra é rebuild do data:, não há mais "missing".
	return { updatedText, changed, missing: [] };
}

export function registerUpdate() {
	return {
		command: "update <project> <env>",
		describe: "Sobrescreve envs no configmap.yml local a partir do Google Sheet",
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
			.option("configmap-path", {
				type: "string",
				describe: "Caminho local do configmap.yml (será sobrescrito)",
				demandOption: true,
			})
			.option("dry-run", {
				type: "boolean",
				describe: "Não grava arquivo, só imprime o que mudaria",
				default: false,
			}),
		handler: async (argv) => {
			const project = argv.project.toString().trim();
			const envName = argv.env.toString().trim();
			const projectKey = normalizeProjectKey(project);

			const configmapPath = normalizeValue(argv["configmap-path"]);
			if (!configmapPath) throw new Error("--configmap-path é obrigatório.");

			const dryRun = argv["dry-run"] === true;

			const googleCsvText = await downloadGoogleCsvText({ projectKey });
			const googleRows = parseCsvText(googleCsvText);
			const googleData = buildGoogleItemsBySection({ googleRows, envName });

			const desired = buildDesiredKeyToValue({ googleData, envName });

			const originalYaml = requireFile(configmapPath, "passe um caminho válido em --configmap-path");
			const { updatedText, changed, missing } = applyConfigMapUpdates({
				configmapYamlText: originalYaml,
				desired,
			});

			console.log(`\n== update ${chalk.cyan(project)} env=${chalk.cyan(desired.env)} ==`);
			console.log(`arquivo: ${configmapPath}`);
			console.log(`mudanças: ${changed.length}`);

			for (const c of changed.slice(0, 50)) {
				console.log(`- ${c.key}: ${chalk.gray(c.from)} -> ${chalk.green(c.to)}`);
			}
			if (changed.length > 50) console.log(chalk.gray(`... +${changed.length - 50} mudanças`));

			if (missing.length) {
				console.log(`\n== chaves não encontradas no configmap data: (${missing.length}) ==`);
				for (const k of missing.slice(0, 50)) console.log(`- ${k}`);
				if (missing.length > 50) console.log(chalk.gray(`... +${missing.length - 50} chaves`));
			}

			if (!dryRun) {
				fs.writeFileSync(configmapPath, updatedText, "utf8");
				console.log(chalk.green("\nOK: configmap.yml atualizado."));
			} else {
				console.log(chalk.yellow("\ndry-run: nada foi gravado."));
			}
		},
	};
}