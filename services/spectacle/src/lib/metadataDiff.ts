/**
 * Utility functions for computing semantic diffs between Iceberg metadata.json files
 */

export interface MetadataDiff {
  schemaChanges: SchemaChanges;
  partitionSpecChanges: PartitionSpecChanges;
  propertyChanges: PropertyChanges;
  snapshotChanges: SnapshotChanges;
}

export interface SchemaChanges {
  addedFields: Field[];
  removedFields: Field[];
  modifiedFields: FieldModification[];
  currentSchemaId: number | null;
  previousSchemaId: number | null;
}

export interface Field {
  id: number;
  name: string;
  type: string;
  required: boolean;
  doc?: string;
}

export interface FieldModification {
  field: Field;
  changes: string[];
}

export interface PartitionSpecChanges {
  changed: boolean;
  currentSpecId: number | null;
  previousSpecId: number | null;
  currentSpec: any;
  previousSpec: any;
}

export interface PropertyChanges {
  added: Record<string, string>;
  removed: Record<string, string>;
  modified: Record<string, { oldValue: string; newValue: string }>;
}

export interface SnapshotChanges {
  currentSnapshotId: number | null;
  previousSnapshotId: number | null;
  snapshotIdChanged: boolean;
}

/**
 * Compute semantic diff between two metadata.json objects
 */
export function computeMetadataDiff(
  currentMetadata: string | null,
  previousMetadata: string | null
): MetadataDiff | null {
  if (!currentMetadata) {
    return null;
  }

  try {
    const current = JSON.parse(currentMetadata);
    const previous = previousMetadata ? JSON.parse(previousMetadata) : null;

    return {
      schemaChanges: computeSchemaChanges(current, previous),
      partitionSpecChanges: computePartitionSpecChanges(current, previous),
      propertyChanges: computePropertyChanges(current, previous),
      snapshotChanges: computeSnapshotChanges(current, previous),
    };
  } catch (e) {
    console.error('Error computing metadata diff:', e);
    return null;
  }
}

function computeSchemaChanges(current: any, previous: any | null): SchemaChanges {
  const currentSchemaId = current['current-schema-id'] ?? null;
  const previousSchemaId = previous?.['current-schema-id'] ?? null;

  const currentSchema = findSchemaById(current, currentSchemaId);
  const previousSchema = previous ? findSchemaById(previous, previousSchemaId) : null;

  const addedFields: Field[] = [];
  const removedFields: Field[] = [];
  const modifiedFields: FieldModification[] = [];

  if (currentSchema && previousSchema) {
    const currentFields = extractFieldsMap(currentSchema);
    const previousFields = extractFieldsMap(previousSchema);

    // Find added fields
    currentFields.forEach((field, id) => {
      if (!previousFields.has(id)) {
        addedFields.push(field);
      }
    });

    // Find removed fields
    previousFields.forEach((field, id) => {
      if (!currentFields.has(id)) {
        removedFields.push(field);
      }
    });

    // Find modified fields
    currentFields.forEach((currentField, id) => {
      const previousField = previousFields.get(id);
      if (previousField) {
        const changes = detectFieldChanges(currentField, previousField);
        if (changes.length > 0) {
          modifiedFields.push({ field: currentField, changes });
        }
      }
    });
  } else if (currentSchema && !previousSchema) {
    // All fields are new
    const currentFields = extractFieldsMap(currentSchema);
    currentFields.forEach((field) => {
      addedFields.push(field);
    });
  }

  return {
    addedFields,
    removedFields,
    modifiedFields,
    currentSchemaId,
    previousSchemaId,
  };
}

function findSchemaById(metadata: any, schemaId: number | null): any {
  if (!metadata.schemas || schemaId === null) {
    return null;
  }
  return metadata.schemas.find((s: any) => s['schema-id'] === schemaId);
}

function extractFieldsMap(schema: any): Map<number, Field> {
  const fieldsMap = new Map<number, Field>();
  if (schema.fields) {
    for (const field of schema.fields) {
      fieldsMap.set(field.id, {
        id: field.id,
        name: field.name,
        type: JSON.stringify(field.type),
        required: field.required ?? false,
        doc: field.doc,
      });
    }
  }
  return fieldsMap;
}

function detectFieldChanges(current: Field, previous: Field): string[] {
  const changes: string[] = [];
  
  if (current.name !== previous.name) {
    changes.push(`Name: ${previous.name} → ${current.name}`);
  }
  if (current.type !== previous.type) {
    changes.push(`Type: ${previous.type} → ${current.type}`);
  }
  if (current.required !== previous.required) {
    changes.push(`Required: ${previous.required} → ${current.required}`);
  }
  if (current.doc !== previous.doc) {
    changes.push(`Doc: ${previous.doc || 'none'} → ${current.doc || 'none'}`);
  }
  
  return changes;
}

function computePartitionSpecChanges(current: any, previous: any | null): PartitionSpecChanges {
  const currentSpecId = current['default-spec-id'] ?? null;
  const previousSpecId = previous?.['default-spec-id'] ?? null;

  const currentSpec = findPartitionSpecById(current, currentSpecId);
  const previousSpec = previous ? findPartitionSpecById(previous, previousSpecId) : null;

  const changed = currentSpecId !== previousSpecId || 
                  JSON.stringify(currentSpec) !== JSON.stringify(previousSpec);

  return {
    changed,
    currentSpecId,
    previousSpecId,
    currentSpec,
    previousSpec,
  };
}

function findPartitionSpecById(metadata: any, specId: number | null): any {
  if (!metadata['partition-specs'] || specId === null) {
    return null;
  }
  return metadata['partition-specs'].find((s: any) => s['spec-id'] === specId);
}

function computePropertyChanges(current: any, previous: any | null): PropertyChanges {
  const currentProps = current.properties || {};
  const previousProps = previous?.properties || {};

  const added: Record<string, string> = {};
  const removed: Record<string, string> = {};
  const modified: Record<string, { oldValue: string; newValue: string }> = {};

  // Find added and modified properties
  for (const [key, value] of Object.entries(currentProps)) {
    if (!(key in previousProps)) {
      added[key] = value as string;
    } else if (previousProps[key] !== value) {
      modified[key] = {
        oldValue: previousProps[key],
        newValue: value as string,
      };
    }
  }

  // Find removed properties
  for (const [key, value] of Object.entries(previousProps)) {
    if (!(key in currentProps)) {
      removed[key] = value as string;
    }
  }

  return { added, removed, modified };
}

function computeSnapshotChanges(current: any, previous: any | null): SnapshotChanges {
  const currentSnapshotId = current['current-snapshot-id'] ?? null;
  const previousSnapshotId = previous?.['current-snapshot-id'] ?? null;

  return {
    currentSnapshotId,
    previousSnapshotId,
    snapshotIdChanged: currentSnapshotId !== previousSnapshotId,
  };
}
