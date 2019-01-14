import { RecordBatch } from 'apache-arrow';
import { TField, TSchema, TFieldsMap } from '../interfaces';
export declare function assignNewEmptyColumns<T extends TSchema, R extends TSchema>(newFields: TField<R>[], outerRecordBatch: RecordBatch<T>, outerFieldsMap: TFieldsMap<T>, innerRecordBatch: RecordBatch<R>, innerFieldsMap: TFieldsMap<R>): [RecordBatch<T & R>, TFieldsMap<T & R>];
