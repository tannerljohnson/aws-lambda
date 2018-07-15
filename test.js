'use strict';
const parquet = require('parquetjs');

async function example() {
  let reader = await parquet.ParquetReader.openFile('fruits.parquet');
  console.log(reader);
  reader.close();
}

example();
