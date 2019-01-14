import { mergeFields } from './fields';
import { Schema, RecordBatch } from 'apache-arrow';
import { mergeMaps, findCommonFields } from '../util';
import { TKey, TField, TSchema, TFieldsMap, KeysMap } from '../interfaces';

// todo: extract variable-width column merge from graphistry arrow-util project

export function mergeRecordBatches<T extends TSchema, R extends TSchema>(
    mergeOn: TKey<T & R>, recordBatchIndex: KeysMap,
    outerRecordBatch: RecordBatch<T>, outerFieldsMap: TFieldsMap<T>,
    innerRecordBatch: RecordBatch<R>, innerFieldsMap: TFieldsMap<R>
) : [RecordBatch<T & R>, Int32Array, KeysMap] {

    let numNewRows = 0;
    const length = outerRecordBatch.length;
    const newRowIndices = new Int32Array(innerRecordBatch.length);

    const outerIndexVector = outerRecordBatch.getChildAt(outerFieldsMap.get(mergeOn as TKey<T>)![1])!;
    const innerIndexVector = innerRecordBatch.getChildAt(innerFieldsMap.get(mergeOn as TKey<R>)![1])!;

    if (!recordBatchIndex && (recordBatchIndex = Object.create(null))) {
        for (let i = -1; ++i < length; recordBatchIndex[outerIndexVector.get(i)] = i);
    }

    const commonFields = findCommonFields(outerFieldsMap, innerRecordBatch.schema.fields as TField<T & R>[], mergeOn);

    if (commonFields.length > 0) {
        const names = commonFields.map((f) => f.name);
        const left = outerRecordBatch.select(...names) as RecordBatch<T | R>;
        const right = innerRecordBatch.select(...names) as RecordBatch<T | R>;
        for (let i = -1, n = innerRecordBatch.length; ++i < n;) {
            const innerVal = innerIndexVector.get(i);
            const outerIdx = recordBatchIndex[innerVal]!;
            (outerIdx > -1)
                ? left.set(outerIdx, right.get(i))
                : (newRowIndices[numNewRows++] = i);
        }
    }

    const schema = new Schema(
        mergeFields(outerRecordBatch.schema.fields, innerFieldsMap),
        mergeMaps(new Map(), outerRecordBatch.schema.metadata, innerRecordBatch.schema.metadata)
    );

    return [
        new RecordBatch(schema, outerRecordBatch.data),
        newRowIndices.slice(0, numNewRows), recordBatchIndex
    ];
}
