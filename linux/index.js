/*
 * Lambda function simply takes HTTP POST body input assuming no other
 * relevant data as path, query parameters, etc. and 1) forwards JSON body
 * to s3 and 2) sends a callback to post actor.
 *
 * Runtime: Node.js 8.10
 */

'use strict';
console.log('Loading forward simple json function...');

// set up dependencies
var AWS = require('aws-sdk');
const parquet = require('parquetjs');

// set up s3 dump reqs
let s3bucket = 'your-bucket-name';
var s3key = "test";
let s3data = "your caliper action";

function putObjectToS3(bucket, key, data) {
  var s3 = new AWS.S3();
  var params = {
    Bucket: bucket,
    Key: key,
    Body: data
  };

  s3.putObject(params, function(err, data) {
    if (err) console.log(err, err.stack); // error
    else console.log(data); // success
  });
}

// set up file location using Caliper info
// (test/Year/Month/Day/Source/CaliperSensor)
function getPathname(partitionArray) {
  var path = s3key;
  for (var i=0; i<partitionArray.length; i++) {
    path += "/" + partitionArray[i];
  }
  return path;
}

// Parquet configuration and writing
// TODO: how is this data supposed to even look?
async function writeParquet(mYear, mMonth, mDay, mSource, mCaliperSensor, mCaliperPayload) {
  let schema = new parquet.ParquetSchema({
    year:           { type: 'UTF8' },
    month:          { type: 'UTF8' },
    day:            { type: 'UTF8' },
    source:         { type: 'UTF8' },
    caliper_sensor: { type: 'UTF8' },
    caliper_payload:{ type: 'UTF8' },
  });

  // TODO: you may need to flush this file first.
  let writer = await parquet.ParquetWriter.openFile(schema, 'caliper.parquet');

  await writer.appendRow({
    year: mYear,
    month: mMonth,
    day: mDay,
    source: mSource,
    caliper_sensor: mCaliperSensor,
    caliper_payload: mCaliperPayload
  });

  await writer.close();

}

exports.handler = function(event, context, callback) {
  // test: grab some interesting values from a caliper body
  // assume parent: caliper, child: caliper._index
  let year = '';
  let month = '';
  let day = '';
  let source = '';
  let caliperSensor = '';
  let caliperPayload = '';
  let caliperIndex = '';
  let responseCode = 200;
  console.log("request: " + JSON.stringify(event));

  if (event.body !== null && event.body !== undefined) {
    let body = JSON.parse(event.body);
    caliperPayload = body;

    if (body.caliper) {

      if (body.caliper._index) {
        caliperIndex = body.caliper._index;
      } else {
        console.log("ERR: no body.caliper._index child!");
      }

      if (body.caliper.year) {
        year = body.caliper.year;
      } else {
        console.log("ERR: no body.caliper.year child!");
      }

      if (body.caliper.month) {
        month = body.caliper.month;
      }
      else {
        console.log("ERR: no body.caliper.month child!");
      }

      if (body.caliper.day) {
        day = body.caliper.day;
      }
      else {
        console.log("ERR: no body.caliper.day child!");
      }

      if (body.caliper.source) {
        source = body.caliper.source;
      }
      else {
        console.log("ERR: no body.caliper.source child!");
      }

      if (body.caliper.caliper_sensor) {
        caliperSensor = body.caliper.caliper_sensor;
      }
      else {
        console.log("ERR: no body.caliper.caliper_sensor child!");
      }


    } else {
      console.log("ERR: no body.caliper child!");
    }
  } else {
    console.log("ERR: no event body!");
  }

  // send response to callback
  let verifyToCaller = 'Caliper successfully posted an event. ';
  if (caliperPayload) {
    verifyToCaller += 'data: ' + caliperPayload;
  } else {
    verifyToCaller += 'no data received in body.';
  }
  if (caliperIndex) {
    verifyToCaller += 'It has index: ' + caliperIndex + '!';
  } else {
    verifyToCaller += 'Could not locate index field!';
  }

  // construct response body
  var responseBody = {
    message: verifyToCaller,
    input: event
  };

  // construct response
  var response = {
    statusCode: responseCode,
    headers: {
      "x-custom-header": "test header"
    },
    body: JSON.stringify(responseBody)
  };
  console.log("response: " + JSON.stringify(response));

  // send callback
  callback(null, response);

  // format path
  var pathArr = [ year, month, day, source, caliperSensor ];
  var path = getPathname(pathArr);

  // process data
  writeParquet(year, month, day, source, caliperSensor, caliperPayload);

  // can we just point to the file path instead of reading it?
  // e.g., putObjectToS3(s3bucket, path, './caliper.parquet');

  console.log("s3 path = " + path);
  console.log("action: " + s3data); // delete
  // locate bucket name from env variables
  s3bucket = process.env.bucketName;
  // TODO: retrieve file to send as payload
  putObjectToS3(s3bucket, path, caliperPayload);

  // TODO: delete caliper.parquet
};
