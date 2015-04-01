drop keyspace stream_engine_test;
create keyspace stream_engine_test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};
use stream_engine_test;

source 'ctdpf_ckl_wfp_metadata_recovered.cql';
source 'ctdpf_ckl_wfp_instrument_recovered.cql';
source 'ctdbp_no_calibration_coefficients.cql';
source 'ctdpf_sbe43_sample.cql';
source 'ctdbp_no_sample.cql';
source 'stream_metadata.cql';

copy ctdpf_ckl_wfp_metadata_recovered from 'ctdpf_ckl_wfp_metadata_recovered.csv' with header=true;
copy ctdpf_ckl_wfp_instrument_recovered from 'ctdpf_ckl_wfp_instrument_recovered.csv' with header=true;
copy ctdbp_no_calibration_coefficients from 'ctdbp_no_calibration_coefficients.csv' with header=true;
copy ctdbp_no_sample from 'ctdbp_no_sample.csv' with header=true;
copy ctdpf_sbe43_sample from 'ctdpf_sbe43_sample.csv' with header=true;
copy stream_metadata from 'stream_metadata.csv' with header=true;
