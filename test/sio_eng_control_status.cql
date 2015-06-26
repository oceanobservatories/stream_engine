CREATE TABLE stream_engine_test.sio_eng_control_status (
    subsite text,
    node text,
    sensor text,
    bin int,
    method text,
    time double,
    deployment int,
    id uuid,
    driver_timestamp double,
    ingestion_timestamp double,
    internal_timestamp double,
    port_timestamp double,
    preferred_timestamp text,
    quality_flag text,
    provenance uuid,
    sio_controller_id bigint,
    sio_controller_timestamp bigint,
    sio_eng_clock_drift int,
    sio_eng_number_of_wakeups bigint,
    sio_eng_on_time bigint,
    sio_eng_temperature double,
    sio_eng_voltage double,
    PRIMARY KEY((subsite, node, sensor, bin), method, time, deployment, id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='{"keys":"ALL", "rows_per_partition":"NONE"}' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.000000 AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};