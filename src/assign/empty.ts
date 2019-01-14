import { Schema, RecordBatch } from 'apache-arrow';
import { TData, TField, TSchema, TFieldsMap } from '../interfaces';
import { newEmptyData, assignFieldsMap, fieldToKey } from '../util';

export function assignNewEmptyColumns<T extends TSchema, R extends TSchema>(
    newFields: TField<R>[],
    outerRecordBatch: RecordBatch<T>, outerFieldsMap: TFieldsMap<T>,
    innerRecordBatch: RecordBatch<R>, innerFieldsMap: TFieldsMap<R>
) : [RecordBatch<T & R>, TFieldsMap<T & R>] {

    if (newFields.length <= 0) {
        return [
            <any> outerRecordBatch, outerFieldsMap
        ] as [RecordBatch<T & R>, TFieldsMap<T & R>];
    }

    let vector, length = outerRecordBatch.length;
    const data = outerRecordBatch.data.childData.slice() as TData<T & R>[];
    const fields = outerRecordBatch.schema.fields.slice() as TField<T & R>[];
    
    for (const field of newFields) {
        const key = fieldToKey(field);
        if (outerFieldsMap.has(key as keyof T)) { continue; }
        vector = innerRecordBatch.getChildAt(innerFieldsMap.get(key)![1]);
        if (vector) {
            fields.push(field as TField<T & R>);
            data.push(newEmptyData(vector.data, length) as TData<T & R>);
        }
    }

    return [
        new RecordBatch<T & R>(new Schema(fields, outerRecordBatch.schema.metadata), length, data),
        assignFieldsMap<T & R>(newFields, outerFieldsMap as TFieldsMap<T & R>)
    ];
}
