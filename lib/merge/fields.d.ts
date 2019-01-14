import { Field } from 'apache-arrow';
import { TSchema, TFieldsMap } from '../interfaces';
export declare function mergeFields<T extends TSchema, R extends TSchema>(outerFields: Field[], innerFieldsMap: TFieldsMap<R>): Field<import("apache-arrow").DataType<import("apache-arrow").Type, any>>[];
