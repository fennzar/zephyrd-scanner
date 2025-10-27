import { createGenerator, Config } from "ts-json-schema-generator";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

export interface SchemaTask {
  type: string;
  output: string;
  title?: string;
}

export interface GeneratorOptions {
  expose?: Config["expose"];
  jsDoc?: Config["jsDoc"];
  topRef?: boolean;
  additionalProperties?: boolean;
  skipTypeCheck?: boolean;
}

export interface SchemaConfig {
  tsconfig: string;
  source: string;
  schemas: SchemaTask[];
  options?: GeneratorOptions;
}

export function loadSchemaConfig(configPath: string): SchemaConfig {
  const configJson = readFileSync(configPath, "utf8");
  return JSON.parse(configJson) as SchemaConfig;
}

function ensureDirectory(filePath: string) {
  const dir = dirname(filePath);
  mkdirSync(dir, { recursive: true });
}

function resolveDefinition(definitions: Record<string, any>, definition: any) {
  let current = definition;
  const visited = new Set<string>();

  while (current && typeof current === "object" && typeof current.$ref === "string") {
    const ref: string = current.$ref;
    const match = ref.match(/^#\/definitions\/(.+)$/);
    if (!match) {
      break;
    }
    const nextKey = match[1];
    if (visited.has(nextKey)) {
      break;
    }
    visited.add(nextKey);
    const nextDefinition = definitions[nextKey];
    if (!nextDefinition) {
      break;
    }
    current = nextDefinition;
  }

  return current;
}

function generateSchemaForTask(baseConfig: SchemaConfig, task: SchemaTask) {
  const generatorConfig: Config = {
    path: resolve(baseConfig.source),
    tsconfig: resolve(baseConfig.tsconfig),
    type: task.type,
    expose: baseConfig.options?.expose ?? "export",
    jsDoc: baseConfig.options?.jsDoc ?? "extended",
    topRef: baseConfig.options?.topRef ?? true,
    additionalProperties: baseConfig.options?.additionalProperties ?? false,
    skipTypeCheck: baseConfig.options?.skipTypeCheck ?? false,
  };

  const generator = createGenerator(generatorConfig);
  const schema = generator.createSchema(task.type) as Record<string, any>;

  if (task.title) {
    schema.title ??= task.title;
    schema.description ??= `${task.title} response schema.`;
  }

  const definitions = schema.definitions as Record<string, any> | undefined;
  if (definitions) {
    const mainDefinition = definitions[task.type];
    if (mainDefinition && typeof mainDefinition === "object") {
      if (task.title) {
        mainDefinition.title ??= task.title;
        mainDefinition.description ??= `${task.title} payload.`;
      }
      const resolvedDefinition = resolveDefinition(definitions, mainDefinition);
      if (!schema.type && resolvedDefinition && typeof resolvedDefinition.type === "string") {
        schema.type = resolvedDefinition.type;
      }
    }
  }

  const outputPath = resolve(task.output);
  ensureDirectory(outputPath);
  writeFileSync(outputPath, `${JSON.stringify(schema, null, 2)}\n`, "utf8");
  console.log(`Generated schema for ${task.type} -> ${outputPath}`);
}

export function generateSchemas(configPath = resolve(__dirname, "schema.config.json")) {
  const config = loadSchemaConfig(configPath);

  if (!Array.isArray(config.schemas) || config.schemas.length === 0) {
    console.error("No schemas configured. Update schema/schema.config.json to include a schemas array.");
    process.exit(1);
  }

  for (const task of config.schemas) {
    generateSchemaForTask(config, task);
  }
}

if (require.main === module) {
  generateSchemas();
}
