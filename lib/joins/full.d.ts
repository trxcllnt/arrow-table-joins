import { Table, RecordBatch } from 'apache-arrow';
import { TKey, TField, TSchema } from '../interfaces';
export declare function fullJoin<T extends TSchema, R extends TSchema>(mergeOn: TField<T & R> | TKey<T & R>, target: Table<T> | AsyncIterable<RecordBatch<T>>, source: Table<R> | AsyncIterable<RecordBatch<R>>): AsyncIterableIterator<RecordBatch<T & R>>;
