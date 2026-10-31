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
2. Clone all submodules (`git submodule update --init`)
3. Clone the [ooi-config](https://github.com/oceanobservatories/ooi-config/tree/master) repository (the documentation below assumes it is cloned to the same directory as the `stream-engine` repository clone)
3. Create a `conda` virtual environment with the necessary packages using the `ooi-config/anaconda_env/engine.yml` file:

```shell
conda env create -f ../ooi-config/anaconda_env/engine.yml
```

## Configuration

The default Stream Engine configuration can be found in `config/default.py`.
Overrides can be entered into `config/local.py`. Gunicorn-specific
configuration parameters are set in `gunicorn_config.py`.

## Running

The script `manage-streamng` allows for starting, stopping, reloading and
checking the status of Stream Engine. The restart option combines the stop 
and start options. The reload option will send a HUP signal to gunicorn 
which will terminate all *idle* workers and restart them. Any busy worker 
will continue until the current request is complete. The status option 
returns the process id (PID) of the gunicorn parent process.

```shell
./manage-streamng start
./manage-streamng stop
./manage-streamng restart
./manage-streamng reload
./manage-streamng status
```

Note that the stop behavior is similar to the reload behavior. Any active
workers will continue until their current task is complete. Any new
requests will be rejected but the master gunicorn process will continue
running until all workers are shutdown. Stopping Stream Engine should
generally be avoided unless necessary.

### Running on Test Server

To run on a test server, follow the Prerequisites steps above on that test machine. Then, source the Stream Engine conda environment `engine` and start the service with the `manage-streamng` tool. Run Stream Engine in the `logs` directory as logs are written to the current working directory:

```shell
conda activate engine
cd logs
../manage-streamng start
```

## Logs

The following logs are generated in the `logs` folder:

* `stream_engine.error.log` - General data retrieval, product creation logs
* `stream_engine.access.log` - Gunicorn access logs

## Updating a Stream Engine installation to a newer release

Updating to a new release of Stream Engine is simple, just grab the update,
update your conda environment and the `preload-database` submodule then
reload Stream Engine.

```shell
git pull # or git fetch / git checkout <tag>
git submodule update
conda env update -f ../ooi-config/anaconda_env/engine.yml
./manage-streamng reload
```


## Creating a new Stream Engine release

__Note: this is out of date: the new procedure involves creating an updated ooi conda-forge package from the ooi-config repository.__

1. Update preload database submodule (if needed)
2. Update conda_env.yml with any desired library updates
3. Update config/default.py with the new version
4. Update RELEASE_NOTES with the new version
5. Commit the above changes
6. Tag the commit with the new version

```shell
git tag -a vX.X.X
```

You can then push the commit and the tag to the upstream repo(s):

```shell
git push gerrit master
git push gerrit master --tags
```

## Updating the preload-database submodule (usually only to satisfy unit tests)

1. Within the stream_engine root, change directory to preload-database
2. Ensure all changes you may have are cleared/saved off
3. Run the following commands

```
git fetch origin # assuming "origin" points to the source URL
git rebase origin/master
cd ..  # to stream_engine root
git add preload_database
git commit -m "Issue #nnnnn <message>
git push origin HEAD:nnnnn
```

## Updating the ooi-metadata-service-api submodule

1. Within the stream_engine root, change directory to util/metadata_service/metadata_service_api
2. Ensure all changes you may have are cleared/saved off
3. Run the following commands

```
git fetch origin # assuming "origin" points to the source URL
git rebase origin/master
cd ../../..  # to stream_engine root
git add util/metadata_service/metadata_service_api
git commit -m "Issue #nnnnn <message>
git push origin HEAD:nnnnn
```
## Creating test NetCDF files (used in test_stream_request.py, among others)

NOTE: this was used for 13182,14654 (may be applicable elsewhere)
Ensure up-to-date data has been ingested for the NC files you want to create
Then temporarily modify the stream_engine code as follows to create the files:

1. In util/netcdf_generator.py's _filter_parameters, change default_params to add: sci_water_pressure
2) In util/netcdf_generator.py's _create_files
   a) comment line: ds = rename_glider_lat_lon(stream_key, ds)
   b) after the following code snippet (as of 9/3/2020)
```
                for external_stream_key in self.stream_request.external_includes:
                    for parameter in self.stream_request.external_includes[external_stream_key]:
                        long_parameter_name = external_stream_key.stream_name + "-" + parameter.name
```
      add the following code snippet to ensure these parameters are retained in the output
```
                        if parameter.name in ('m_gps_lat', 'm_gps_lon', 'm_lat', 'm_lon', 'interp_lat', 'interp_lon'):
                            params_to_include.append(long_parameter_name)
                            continue
```
3) Once this is done run a data request against the data to produce the NC files. Then back out these changes.
