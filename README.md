# Stream Engine

## About

Stream Engine is the back-end query engine for the OOI CyberInfrastructure.
It exposes a JSON-based HTTP interface which allows for the querying of
full resolution or subsampled data from the data store and execution of
numerous data product algorithms on this data. The results are returned
to the user in a JSON data structure for synchronous results or
asynchronously as NetCDF4, CSV or TSV.

## Prerequisites

1. Clone the repository
2. Clone all submodules (git submodule update --init)
3. Create a conda virtual environment with the necessary packages:

```
$ conda env create -f conda_env.yml
```

## Configuration

The default Stream Engine configuration can be found in config/default.py.
Overrides can be entered into config/local.py. Gunicorn-specific
configuration parameters are set in gunicorn_config.py.

## Running

The script `manage-streamng` allows for starting, stopping and reloading
Stream Engine. The reload option will send a HUP signal to gunicorn which
will terminate all *idle* workers and restart them. Any busy worker will
 continue until the current request is complete.

```
$ ./manage-streamng start
$ ./manage-streamng stop
$ ./manage-streamng reload
```

Note that the stop behavior is similar to the reload behavior. Any active
workers will continue until their current task is complete. Any new
requests will be rejected but the master gunicorn process will continue
running until all workers are shutdown. Stopping Stream Engine should
generally be avoided unless necessary.

## Logs

The following logs are generated in the logs folder:

* stream_engine.error.log - General data retrieval, product creation logs
* stream_engine.access.log - Gunicorn access logs

## Updating a Stream Engine installation to a newer release

Updating to a new release of Stream Engine is simple, just grab the update,
update your conda environment and the preload database submodule then
reload Stream Engine.

```
$ git pull # or git fetch / git checkout <tag>
$ git submodule update
$ conda env update -f conda_env.yml
$ ./manage-streamng reload
```


## Creating a new Stream Engine release

1. Update preload database submodule (if needed)
2. Update conda_env.yml with any desired library updates
3. Update config/default.py with the new version
4. Update RELEASE_NOTES with the new version
5. Commit the above changes
6. Tag the commit with the new version

```
$ git tag -a vX.X.X
```

You can then push the commit and the tag to the upstream repo(s):

```
$ git push gerrit master
$ git push gerrit master --tags
```
