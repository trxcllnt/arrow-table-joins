import { Buffers } from 'apache-arrow/data';
import { Data, Field, DataType } from 'apache-arrow';
import { TKey, TField, TSchema, TFieldsMap } from './interfaces';

export function fieldToKey(field: Field) { return field.name; }

export function newNullBitmap(length: number, fill = false) {
    return new Uint8Array(((length + 63) & ~63) >>> 3).fill(fill ? 255 : 0);
}

export function newEmptyData<T extends DataType>(data: Data<T>, length: number): Data<T> {
    const { type, values, valueOffsets, typeIds, childData } = data;
    const buffers = <any> [,, newNullBitmap(length)] as Buffers<T>;
    valueOffsets && (buffers[0] = new Int32Array(length + 1));
    values       && (buffers[1] = new data.ArrayType(length));
    typeIds      && (buffers[3] = new Int32Array(length));
    return data.clone(
        type, 0, length, length, buffers,
        childData.map((d) => newEmptyData(d, length)));
}

export function assignFieldsMap<T extends TSchema>(fields: Field[] = [], map: TFieldsMap<T> = new Map(), offset = map.size) {
    return fields.reduce((map, f, i) => map.has(fieldToKey(f))
            ? map : map.set(fieldToKey(f), [f as TField<T>, offset + i]),
        map || new Map()
    );
}

export function mergeMaps<TKey, TVal>(target: Map<TKey, TVal> = new Map(), ...sources: Map<TKey, TVal>[]) {
    return sources.reduce((target, source) => {
        source.forEach((val, key) => target.set(key, val));
        return target;
    }, target || new Map());
}

export function findNewFields<T extends TSchema, R extends TSchema>(map: TFieldsMap<T>, fields: TField<R>[], mergeOn: TKey<T & R>) {
    return fields.filter((f: TField<R> | TKey<T & R>) => !map.has(f = fieldToKey(f as TField<R>)) && (f !== mergeOn));
}

export function findCommonFields<T extends TSchema, R extends TSchema>(map: TFieldsMap<T>, fields: TField<R>[], mergeOn: TKey<T & R>) {
    return fields.filter((f: TField<R> | TKey<T & R>) => map.has(f = fieldToKey(f as TField<R>)) && (f !== mergeOn));
}

export function findMergeOnKey<T extends TSchema, R extends TSchema>(mergeOn: TField<T & R> | TKey<T & R>, fieldsMap: TFieldsMap<T & R>) {
    if (!(mergeOn instanceof Field)) {
        for (let [, [f]] of fieldsMap) {
            if (f.name === mergeOn) { return fieldToKey(f as TField<T & R>); }
        }
        if (!(mergeOn instanceof Field)) {
            throw new ReferenceError(`'mergeOn' must be an Arrow Field or the name of a field in the target table. Received ${mergeOn}`);
        }
    }
    return fieldToKey(mergeOn);
}
