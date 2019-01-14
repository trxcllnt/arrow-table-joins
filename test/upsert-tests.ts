import { fullJoin } from '../src';
import * as generate from './generate';
import {
    Table, Schema, Field,
    DataType, Int32, Float32,
    RecordBatchStreamReader,
    RecordBatchStreamWriter,
} from 'apache-arrow';

describe('fullJoin', () => {

    it('joins fixed-width columns and assigns new cols', async () => {

        const schema1 = new Schema([new Field('id', new Int32()), new Field('i32', new Int32())]);
        const schema2 = new Schema([new Field('id', new Int32()), new Field('f32', new Float32())]);

        const { table: table1, ...table1Data } = generate.table([20], schema1);
        const { table: table2, ...table2Data } = generate.table([10, 10], schema2);

        const ids1 = table1.getColumn('id')!, ids2 = table2.getColumn('id')!;
        for (let i = -1, n = table1.length; ++i < n;) { ids1.set(i, i); ids2.set(i, i + 10); }

        const itr = fullJoin('id', table1, Table.from(table2.serialize()));
        const acutal = await Table.from(RecordBatchStreamWriter.writeAll(itr));

        let rows1 = (table1Data.rows() as [number, number, null][]);
        let rows2 = (table2Data.rows() as [number, null, number][]);

        const nulls = [null, null, null];
        const expected = rows1.slice(0, 10).map(([id, i32]) => [id, i32, null])
            .concat(rows2.map(([id, f32]) => [id, (rows1[id] || nulls)[1], f32]));
        
        validateTable(expected, acutal);
    });

    it('joins fixed-width columns and appends new rows', async () => {

        const schema = new Schema([
            new Field('id', new Int32()),
            new Field('i32', new Int32()),
            new Field('f32', new Float32()),
        ]);

        const { table: table1, ...table1Data } = generate.table([20], schema);
        const { table: table2, ...table2Data } = generate.table([10, 10], schema);

        const ids1 = table1.getColumn('id')!, ids2 = table2.getColumn('id')!;
        for (let i = -1, n = table1.length; ++i < n;) { ids1.set(i, i); ids2.set(i, i + 10); }

        const stream = fullJoin('id', table1,
            RecordBatchStreamWriter.writeAll(table2)
                .pipe(RecordBatchStreamReader.throughNode()));

        const actual = await Table.from(RecordBatchStreamWriter.writeAll(stream));
        const expected = table1Data.rows().slice(0, 10).concat(table2Data.rows());

        validateTable(expected, actual);
    });
});

function validateTable<T extends { [key: string]: DataType; }>(expected: T[], actual: Table<T>) {
    expect(expected.length).toBe(actual.length);
    for (let i = -1, n = actual.length; ++i < n;) {
        expect(actual.indexOf(expected[i] as Table<T>['TValue'])).toBe(i);
    }
}
