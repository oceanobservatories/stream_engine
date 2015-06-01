CREATE TABLE stream_engine_test.ctdpf_ckl_wfp_instrument_recovered (
  subsite text,
  node text,
  sensor text,
  method text,
  time double,
  deployment int,
  id uuid,
  conductivity int,
  driver_timestamp double,
  ingestion_timestamp double,
  internal_timestamp double,
  port_timestamp double,
  preferred_timestamp text,
  pressure int,
  provenance int,
  quality_flag text,
  temperature int,
  PRIMARY KEY ((subsite, node, sensor), method, time, deployment, id)
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
