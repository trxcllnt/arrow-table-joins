import { Field } from 'apache-arrow';
import { mergeMaps, fieldToKey } from '../util';
import { TField, TSchema, TFieldsMap } from '../interfaces';

export function mergeFields<T extends TSchema, R extends TSchema>(outerFields: Field[], innerFieldsMap: TFieldsMap<R>) {
    return outerFields.map((outer) => {
        const key = fieldToKey(outer);
        if (!innerFieldsMap.has(key)) {
            return outer;
        }
        const [inner] = innerFieldsMap.get(key)!;
        return new Field(
            outer.name, outer.type,
            outer.nullable || inner.nullable,
            mergeMaps(new Map(), outer.metadata!, inner.metadata!)
        ) as TField<T & R>;
    }, [] as TField<T & R>[]);
}
