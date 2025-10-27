import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { generateSchemas, loadSchemaConfig, SchemaTask } from "./generateSchemas";

type JsonSchema = Record<string, any>;

interface RenderContext {
  definitions: Record<string, any>;
  renderedKeys: Set<string>;
}

interface ResolvedSchema {
  schema: any;
  refName?: string;
}

function extractRefName(ref?: string): string | undefined {
  if (!ref || typeof ref !== "string") {
    return undefined;
  }
  const match = ref.match(/^#\/definitions\/(.+)$/);
  return match ? match[1] : undefined;
}

function resolveSchemaNode(node: any, definitions: Record<string, any>, fallbackRefName?: string): ResolvedSchema {
  if (!node || typeof node !== "object") {
    return { schema: node ?? {}, refName: fallbackRefName };
  }

  if (typeof node.$ref === "string") {
    const refName = extractRefName(node.$ref) ?? fallbackRefName;
    if (refName && definitions[refName]) {
      return resolveSchemaNode(definitions[refName], definitions, refName);
    }
    return { schema: node, refName };
  }

  return { schema: node, refName: fallbackRefName };
}

function describeType(resolved: ResolvedSchema, definitions: Record<string, any>): string {
  const schema = resolved.schema ?? {};
  const refName = resolved.refName;
  const schemaType = schema.type;

  if (schema.enum && Array.isArray(schema.enum) && schema.enum.length > 0) {
    const baseType = Array.isArray(schemaType) ? schemaType.join(" | ") : schemaType ?? "enum";
    return `${baseType} (enum: ${schema.enum.join(", ")})`;
  }

  if (Array.isArray(schemaType)) {
    return schemaType.join(" | ");
  }

  if (schemaType === "array") {
    const itemResolved = resolveSchemaNode(schema.items ?? {}, definitions);
    const itemType = describeType(itemResolved, definitions);
    return `array<${itemType}>`;
  }

  if (schemaType === "object" || schema.properties) {
    return refName ? `object (${refName})` : "object";
  }

  if (schemaType) {
    return schemaType;
  }

  if (schema.anyOf) {
    return schema.anyOf
      .map((entry: any) => describeType(resolveSchemaNode(entry, definitions), definitions))
      .join(" | ");
  }

  if (schema.oneOf) {
    return schema.oneOf
      .map((entry: any) => describeType(resolveSchemaNode(entry, definitions), definitions))
      .join(" | ");
  }

  if (refName) {
    return refName;
  }

  return "unknown";
}

function renderSchemaNode(
  label: string,
  node: any,
  context: RenderContext,
  lines: string[],
  depth: number,
  key: string
) {
  const resolved = resolveSchemaNode(node, context.definitions);
  const schema = resolved.schema ?? {};

  if (schema.type === "array") {
    renderArray(label, schema, resolved, context, lines, depth, key);
    return;
  }

  if (schema.type === "object" || schema.properties) {
    const effectiveKey = resolved.refName ?? key;
    if (context.renderedKeys.has(effectiveKey)) {
      return;
    }
    context.renderedKeys.add(effectiveKey);
    renderObject(resolved.refName ?? label, schema, context, lines, depth, effectiveKey);
  }
}

function renderArray(
  label: string,
  schema: any,
  resolved: ResolvedSchema,
  context: RenderContext,
  lines: string[],
  depth: number,
  key: string
) {
  const headingLevel = Math.min(6, depth + 3);
  const headingTitle = depth === 0 ? "Items" : `${label} Items`;
  const itemsResolved = resolveSchemaNode(schema.items ?? {}, context.definitions);
  lines.push(`${"#".repeat(headingLevel)} ${headingTitle}`, "");
  lines.push(`Type: ${describeType(itemsResolved, context.definitions)}`, "");

  if (
    itemsResolved.schema &&
    (itemsResolved.schema.type === "object" || itemsResolved.schema.properties || itemsResolved.schema.type === "array")
  ) {
    const nestedKey = itemsResolved.refName ?? `${key}[]`;
    if (!context.renderedKeys.has(nestedKey)) {
      renderSchemaNode(itemsResolved.refName ?? `${label} item`, schema.items ?? {}, context, lines, depth + 1, nestedKey);
    }
  }
}

function renderObject(
  label: string,
  schema: any,
  context: RenderContext,
  lines: string[],
  depth: number,
  key: string
) {
  const properties = schema.properties ?? {};
  const entries = Object.entries(properties);
  const headingLevel = Math.min(6, depth + 3);
  const headingTitle = depth === 0 ? "Fields" : `${label} Fields`;

  lines.push(`${"#".repeat(headingLevel)} ${headingTitle}`, "");

  if (entries.length === 0) {
    lines.push("_No properties_", "");
    return;
  }

  lines.push("| Field | Type | Required | Description |");
  lines.push("| ----- | ---- | -------- | ----------- |");

  const requiredSet = new Set<string>(Array.isArray(schema.required) ? schema.required : []);
  const nestedQueue: Array<{ label: string; schemaNode: any; key: string }> = [];

  for (const [propName, propSchema] of entries) {
    const resolvedProp = resolveSchemaNode(propSchema, context.definitions);
    const typeLabel = describeType(resolvedProp, context.definitions);
    const required = requiredSet.has(propName) ? "yes" : "no";
    const description = (resolvedProp.schema?.description ?? "").replace(/\r?\n/g, " ").trim();

    lines.push(`| ${propName} | ${typeLabel} | ${required} | ${description} |`);

    if (
      resolvedProp.schema &&
      (resolvedProp.schema.type === "object" || resolvedProp.schema.properties || resolvedProp.schema.type === "array")
    ) {
      const nestedKey = resolvedProp.refName ?? `${key}.${propName}`;
      if (!context.renderedKeys.has(nestedKey)) {
        nestedQueue.push({
          label: resolvedProp.refName ?? propName,
          schemaNode: propSchema,
          key: nestedKey,
        });
      }
    }
  }

  lines.push("");

  for (const nested of nestedQueue) {
    if (!context.renderedKeys.has(nested.key)) {
      renderSchemaNode(nested.label, nested.schemaNode, context, lines, depth + 1, nested.key);
    }
  }
}

function loadRootSchema(schema: JsonSchema, task: SchemaTask): { node: any; refName?: string } {
  const definitions = schema.definitions ?? {};

  if (schema.$ref) {
    const refName = extractRefName(schema.$ref);
    if (refName && definitions[refName]) {
      return { node: definitions[refName], refName };
    }
  }

  if (definitions[task.type]) {
    return { node: definitions[task.type], refName: task.type };
  }

  return { node: schema, refName: task.type };
}

function renderEndpoint(task: SchemaTask, schemaJson: JsonSchema): string[] {
  const { node: rootNode, refName } = loadRootSchema(schemaJson, task);
  const definitions = schemaJson.definitions ?? {};
  const rootResolved = resolveSchemaNode(rootNode, definitions, refName ?? task.type);

  const context: RenderContext = {
    definitions,
    renderedKeys: new Set<string>(),
  };

  const lines: string[] = [];
  lines.push(`**Type:** ${describeType(rootResolved, definitions)}`);

  const description =
    rootResolved.schema?.description ?? schemaJson.description ?? `${task.title ?? task.type} payload reference.`;
  if (description) {
    lines.push("", description, "");
  } else {
    lines.push("");
  }

  const rootKey = rootResolved.refName ?? task.type;
  renderSchemaNode(rootResolved.refName ?? task.title ?? task.type, rootNode, context, lines, 0, rootKey);

  return lines;
}

function buildSectionHeading(task: SchemaTask): string {
  if (task.title) {
    return task.title;
  }
  const relativePath = relative(process.cwd(), task.output).replace(/\.schema\.json$/i, "");
  return `${task.type}${relativePath ? ` (${relativePath})` : ""}`;
}

function generateDocumentation() {
  const configPath = resolve(__dirname, "schema.config.json");
  generateSchemas(configPath);
  const config = loadSchemaConfig(configPath);

  const outputPath = resolve(__dirname, "../docs/api.md");
  const docsDir = dirname(outputPath);
  if (!existsSync(docsDir)) {
    mkdirSync(docsDir, { recursive: true });
  }

  const sections: string[] = [];
  sections.push("# Zephyrd Scanner API", "");
  sections.push(`Generated ${new Date().toISOString()} from TypeScript definitions.`, "", "---", "");

  for (const task of config.schemas) {
    const schemaPath = resolve(process.cwd(), task.output);
    if (!existsSync(schemaPath)) {
      throw new Error(`Schema file not found: ${schemaPath}`);
    }

    const heading = buildSectionHeading(task);
    const schemaJson = JSON.parse(readFileSync(schemaPath, "utf8")) as JsonSchema;
    const endpointLines = renderEndpoint(task, schemaJson);

    sections.push(`## ${heading}`, "", ...endpointLines, "", "---", "");
  }

  while (sections.length > 0 && sections[sections.length - 1] === "") {
    sections.pop();
  }
  if (sections[sections.length - 1] === "---") {
    sections.pop();
  }

  writeFileSync(outputPath, `${sections.join("\n")}\n`, "utf8");
  console.log(`API documentation written to ${relative(process.cwd(), outputPath)}`);
}

generateDocumentation();
