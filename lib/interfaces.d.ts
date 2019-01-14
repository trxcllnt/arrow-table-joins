import { Data, Field, DataType, RecordBatch } from 'apache-arrow';
export declare type TSchema = {
    [key: string]: DataType;
};
export declare type TKey<T extends TSchema> = keyof T;
export declare type TData<T extends TSchema> = Data<T[keyof T]>;
export declare type TField<T extends TSchema> = Field<T[keyof T]>;
export declare type KeysMap = {
    [key: number]: number;
    [key: string]: number;
};
export declare type TFieldsMap<T extends TSchema> = Map<TKey<T>, [TField<T>, number]>;
export interface FullJoinState<T extends TSchema, R extends TSchema> {
    mergeOn: TField<T & R> | TKey<T & R>;
    newRows: [Int32Array, RecordBatch<T & R>][];
    mergeOnKey?: TKey<T & R>;
    outerFieldsMap?: TFieldsMap<T & R>;
    innerFieldsMap?: TFieldsMap<R>;
    recordBatchKeyMaps: KeysMap[];
    recordBatches: AsyncIterable<RecordBatch<T & R>>;
}
