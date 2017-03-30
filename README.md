# Stream Engine

## About

Stream Engine is the back-end query engine for the OOI CyberInfrastructure. It exposes a JSON-based
HTTP interface which allows for the querying of full resolution or subsampled data from the data
store and execution of numerous data product algorithms on this data. The results are returned to
the user in a JSON data structure for synchronous results or asynchronously as NetCDF4, CSV or TSV.

## Prerequisites

1. Clone the repository
2. Create a conda virtual environment with the necessary packages:

```
$ conda env create -f conda_env.yml
```

## Configuration

The default Stream Engine configuration can be found in config/default.py. Overrides can be entered
into config/local.py. Gunicorn-specific configuration parameters are set in gunicorn_config.py.

## Running

The script `manage-streamng` allows for starting, stopping and reloading Stream Engine. The reload
option will send a HUP signal to gunicorn which will terminate all *idle* workers and restart them.
Any busy worker will continue until the current request is complete.

```
$ ./manage-streamng start
$ ./manage-streamng stop
$ ./manage-streamng reload
```

Note that the stop behavior is similar to the reload behavior. Any active workers will continue
until their current task is complete. Any new requests will be rejected but the master gunicorn
process will continue running until all workers are shutdown.

## Logs

The following logs are generated in the logs folder:

* stream_engine.error.log - General data retrieval, product creation logs
* stream_engine.access.log - Gunicorn access logs
