### streaming in-place Arrow Table joins

For now only the full outer join over fixed-width columns is implemented.

```js
import { joinFull } from 'arrow-table-joins';
import { Table, RecordBatchStreamReader } from 'apache-arrow';

const lilTableStream = RecordBatchStreamReader.from(getLilTable());
const bigTableStream = RecordBatchStreamReader.from(getBigTable());

const joinedRecordBatches = [];
// join the values of `lilTable` into `bigTable` in-place
for await (const recordBatch of joinFull('id', bigTable, lilTable)) {
    joinedRecordBatches.push(recordBatch);
}

const joinedTable = new Table(joinedRecordBatches);

console.log(joinedTable);
```
