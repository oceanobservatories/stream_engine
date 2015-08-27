create table stream_engine_test.partition_metadata
( stream text, refdes text, method text, bin int, store text, first double, last double, count bigint,
primary key ((stream, refdes), method, bin, store));

