import { RecordBatch } from 'apache-arrow';
import { TKey, TSchema, TFieldsMap, KeysMap } from '../interfaces';
export declare function mergeRecordBatches<T extends TSchema, R extends TSchema>(mergeOn: TKey<T & R>, recordBatchIndex: KeysMap, outerRecordBatch: RecordBatch<T>, outerFieldsMap: TFieldsMap<T>, innerRecordBatch: RecordBatch<R>, innerFieldsMap: TFieldsMap<R>): [RecordBatch<T & R>, Int32Array, KeysMap];
