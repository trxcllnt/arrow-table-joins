import { AsyncIterable as AsyncIterableX } from 'ix';
import { Schema, Table, RecordBatch } from 'apache-arrow';

import { assignNewEmptyColumns } from '../assign/empty';
import { mergeRecordBatches } from '../merge/recordbatches';
import { findNewFields, findMergeOnKey, assignFieldsMap } from '../util';
import { TKey, TField, TSchema, KeysMap, FullJoinState, TFieldsMap } from '../interfaces';

export async function* fullJoin<T extends TSchema, R extends TSchema>(
    mergeOn: TField<T & R> | TKey<T & R>,
    target: Table<T> | AsyncIterable<RecordBatch<T>>,
    source: Table<R> | AsyncIterable<RecordBatch<R>>
): AsyncIterableIterator<RecordBatch<T & R>> {
    
    const inners = AsyncIterableX.as(source instanceof Table ? source.chunks : source);
    const outers = AsyncIterableX.as(target instanceof Table ? target.chunks : target);
    const { newRows, recordBatches, outerFieldsMap } = await inners.reduce<RecordBatch<R>, FullJoinState<T, R>>(
        fullJoinInner, { mergeOn, newRows: [], recordBatches: <any> outers, recordBatchKeyMaps: [] } as FullJoinState<T, R>);

    let numNewRows: number;
    let schema: Schema | null = null;
    let batch: RecordBatch<T & R> | null = null;
    for await (batch of recordBatches) {
        yield (batch = new RecordBatch(schema || (schema = batch.schema), batch.data));
    }

    if (!batch || !schema || newRows.length <= 0) { return; }
    if ((numNewRows = newRows.map(([xs]) => xs.length).reduce((x, y) => x + y, 0)) <= 0) { return; }

    ([batch] = assignNewEmptyColumns<T, T & R>(
        [...outerFieldsMap!.entries()].map(([, [f]]) => f),
        new RecordBatch(new Schema([]), numNewRows, []), new Map(),
        batch, outerFieldsMap!,
    ));

    let i = -1;
    for (const [idxs, inner] of newRows) {
        for (let j = -1, n = idxs.length; ++j < n;) {
            batch.set(++i, inner.get(idxs[j])!);
        }
    }

    yield new RecordBatch(schema, batch.data);
}

async function fullJoinInner<T extends TSchema, R extends TSchema>(
    state: FullJoinState<T, R>,
    inner: RecordBatch<R>
) {

    let newRowIndices: Int32Array;
    let newFields: TField<R>[] = [];

    let outerRecordBatchIndex = -1;
    let mergeOn = state.mergeOnKey;
    let recordBatch: RecordBatch<T & R>;
    let outerFieldsMap = state.outerFieldsMap;
    let innerFieldsMap = state.innerFieldsMap;
    
    let recordBatches = [];
    let recordBatchKeysMap: KeysMap;
    let recordBatchKeyMaps = state.recordBatchKeyMaps;
    
    if (outerFieldsMap && mergeOn) {
        newFields = findNewFields(outerFieldsMap, inner.schema.fields as TField<R>[], mergeOn);
    }

    for await (const outer of state.recordBatches) {

        if (outer.schema === inner.schema) {
            recordBatches.push(outer);
            continue;
        }

        if (!mergeOn || !outerFieldsMap || !innerFieldsMap) {
            ([outerFieldsMap, innerFieldsMap, mergeOn] = createFullJoinState<T, R>(state, outer, inner));
            if (mergeOn && !outerFieldsMap.has(mergeOn as keyof T)) { continue; }
            if (!mergeOn || !innerFieldsMap.has(mergeOn as keyof R)) { return state; }
            newFields = findNewFields(outerFieldsMap, inner.schema.fields as TField<R>[], mergeOn);
        }

        ([recordBatch, outerFieldsMap] = assignNewEmptyColumns<T & R, R>(newFields, outer, outerFieldsMap, inner, innerFieldsMap));

        ([recordBatch, newRowIndices, recordBatchKeysMap] = mergeRecordBatches<T & R, R>(
            mergeOn,
            recordBatchKeyMaps[++outerRecordBatchIndex],
            recordBatch, outerFieldsMap, inner, innerFieldsMap
        ));

        recordBatches[outerRecordBatchIndex] = recordBatch;
        recordBatchKeyMaps[outerRecordBatchIndex] = recordBatchKeysMap;
        newRowIndices.length > 0 && state.newRows.push([newRowIndices, <any> inner as RecordBatch<T & R>]);
    }

    if (outerRecordBatchIndex === -1) {
        recordBatches.push(<any> inner as RecordBatch<T & R>);
    }

    state.recordBatches = AsyncIterableX.as(recordBatches);

    return state;
}

function createFullJoinState<T extends TSchema, R extends TSchema>(
    state: FullJoinState<T, R>,
    outer: RecordBatch<T & R>, inner: RecordBatch<R>
) {
    const outerFieldsMap = state.outerFieldsMap = assignFieldsMap(outer.schema.fields, state.outerFieldsMap);
    const innerFieldsMap = state.innerFieldsMap = assignFieldsMap(inner.schema.fields, state.innerFieldsMap);
    const mergeOn = state.mergeOnKey || (state.mergeOnKey = findMergeOnKey<T, R>(state.mergeOn, outerFieldsMap));
    return [outerFieldsMap, innerFieldsMap, mergeOn] as [TFieldsMap<T & R>, TFieldsMap<R>, TKey<T | R>];
}
