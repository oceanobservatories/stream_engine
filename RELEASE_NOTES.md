# Stream Engine

# Release 1.17.0 2020-09-10

Issue #14828 - Use unicode for qartod_results variables to get encoding as array of strings

Issue #14787 - Preload update to use pressure_depth for dosta calculations when appropriate

Issue #14654 - Add processing of interp_lat,lon parameters

Issue #14675 - Compute depth from pressure

# Release 1.16.0 2020-08-11

Issue #14791 - Correct pressure_depth/int_ctd_pressure handling

Issue #14790 - Add support for QARTOD climatology tests

# Release 1.15.1 2020-06-12

Issue #14768 - pressure_depth has not changed: fixed renaming

# Release 1.15.0 2020-06-09

Issue #14398 - QARTOD proof of concept
- Add QARTOD QC executor
- Add tests for QARTOD changes
- Update conda environment/requirements

Issue #14278 - Use netcdf_name attribute to rename parameters in output files

Issue #13674 - Exclude coordinates attribute from timestamp parameters
- Added *timestamp* variables to NETCDF_NONSCI_VARIABLES config list

Issue #14540 - Provide avenue to correct stream key references in particle data
- Script provided to manage obtaining current data and applying correct refdes values to it

# Release 1.14.0 2020-04-07

Issue #13402 - Handle alternative data dimensions
- Resolve streams with minor method name variations (e.g. recovered_host vs recovered_inst)
- Handle non-obs dimensions when building up datasets
- Provide concatenation over multiple dimensions to handle non-obs variables
- Move custom concatenation logic to new file multi_concat.py

# Release 1.13.0 2020-03-04

Issue #14535 - Parameter naming conflict prevents netcdf generation

# Release 1.12.0 2020-02-04

Issue #14573 - Do not interpolate raw data across deployments for the same instrument

Issue #14387 - pCO2 sensors not returning recent data
- Select extra data points for supporting streams immediately before and after the
  request time range for better interpolation. Restrict these extra data points
  to be from deployments that are within the request time range.

# Release 1.11.0 2019-12-04

Issue #14304 - Add bin size entry for new metbk_ct_dcl_instrument stream

Issue #14518 - Allow optional args during derived product function arg binding
- Allow function to be called even when an argument to the function was resolved but
  it did not contain any data if the argument was specified as optional in preload

# Release 1.10.0 2019-09-06

Issue #9291 - Apply lat,lon on fixed assets to netcdf global attrs
- On fixed assets add lat,lon to global attrs, remove from variable list

Issue #11399 - Ability to specify function parameter as optional
- Allows calling a parameter function pointing to a Python function
  which specifies a default for the optional argument

Issue #12816 - Add version endpoint with dependency info

Issue #13182 - Async data download behaving differently on the Data Navigation tab than on the Plotting tab
- Add capture,reporting of m_lat,m_lon parameters from glider_gps_position stream

# Release 1.9.0 2019-06-06

Issue #14159 - Record data download size and time data
- Add endpoint to measure output directory size and write time
- Update directory checking regex to handle new style directory names
- Fix error handling to keep StreamEngineException status codes

# Release 1.8.0 2019-01-04

Issue #13501 - Handle mismatched coordinates in NetCDF aggregation

# Release 1.7.0 2018-07-26
Issue #13101 - Replace simple aggregation timeout with inactivity timeout
- Remove AGGREGATION_TIMEOUT_SECONDS config value
- Add DEFAULT_MAX_RUNTIME, DEFAULT_ACTIVITY_POLL_PERIOD, AGGREGATION_MAX_RUNTIME, and 
  AGGREGATION_ACTIVITY_POLL_PERIOD config values
- Create set_inactivity_timeout decorator and apply to aggregation function

# Release 1.6.0 2018-07-20

Issue #13448 - Prevent getting fill values for wavelength coordinate
- Correct code error that led to NaNs for wavelength values
- Add handling to replace coordinate fill values with whole number sequence

Issue #13299 - Asychronous download of specific parameters to CSV and NetCDF is Inconsistent
- Implemented parameter filtering as part of CVS packaging of data requests.
- Implemented QC parameter filtering in NetCDF packaging of data requests.

Issue #13063 - Parse the stream engine version from the release notes
- Read the version from the release notes at runtime, use this for output to netcdf
  also display version in log when starting up

Issue #13311 - Apply annotation masks in case of open-ended annotations

Issue #13276 - Run qc in separate processes and detect empty directory aggregation
- Handle qc errors like segfaults by running qc in process and detecting if the
  process dies without writting results to pipe
- Add checks in aggregation so that status.json file records attempts to aggregate
  a directory with no files in it or a directory with no status files
- Fix gunicorn compatibility issue with the above changes by using os.fork instead
  of the python multiprocessing module
- Make parent process wait for child to prevent zombie processes

# Release 1.5.0 2018-04-16

Issue #13126 - Restore stream key data to annotation filenames
- Add stream key back to annotation filenames and prevent duplicate annotations
  for request with multiple streams

Issue #13284 - Exclude parser quality_flag from NetCDF files

Issue #13264 - Removed the unused depth variables from NetCDF list

Issue #13153 - Fix return code for No Data to be 404 - Not Found
- This fix should also fix other errors that were always getting
  returned under 500 code instead of a more appropriate code.

Issue #13234 - Stop filtering qc parameters from NetCDF files
- Add logic to prevent 'qc_executed' and 'qc_results' suffixed parameters from being
  filtered out of NetCDF file data despite their absence from request parameter list

Issue #13185 - Correct interpolation with virtual source
- Interpolate needed parameters twice: once for parameters from non-virtual
  sources and again later for parameters from virtual sources after those
  virtual sources are calculated

Issue #13126 - Include open ended and data gap annotations
- async requests write annotation files even if no particle data is found
- annotation data is aggregated to a single annotation.json file
- open ended annotation are included in annotation.json file
- async JSON responses return particle and annotation data

Issue #12879 - Prevent data without deployment info from returning
- add require_deployment query parameter and default configuration setting

By default, data without deployments will not be returned via data requests.
The default behavior is configured via the REQUIRE_DEPLOYMENT variable in default.py

To override the default behavior on a per request basis, set the query parameter
require_deployment=false when making a data request.

Issue #9328 - Make interpolated ctd pressure available
- add interpolated ctd pressure to the data request
- ensure interpolated ctd pressure is included in NetCDF and CSV

# Release 1.4.1 2018-02-13

Issue #13195 - Fix empty datasets with custom dims not being handled correctly

# Release 1.4.00 2018-02-02

Issue #12001 - Do not open provenance/annotation files in append mode

Issue #13025 - Add support for custom dimensioned data
- add support for custom dimensional data sets
- add support for __builtin__ PythonFunction in preload database,

  functions should expect inputs in 2d arrays of [obs,data]
  and output data as 2d array of [obs,data]

  functions should be valid Python syntax and use only standard
  Python built-in functions

  for example, see the enumerate function in preload database
  ParameterFunctions.csv:

  functiontype: PythonFunction
  name: enumerate
  owner: __builtin__
  function: [[idx for idx, _ in enumerate(x[0])]]
  args: ['x']

Issue #12715 - Standardize status file naming

Issue #12004 - Add aggregation exception handling

Issue #10745 - Add NetCDF depth coordinate and associated attributes

# Release 1.3.10 2017-12-14

Issue #12916 - Enable starting stream_engine from any folder

Issue #12515 - Use preload data from postgres

# Release 1.3.9 2017-11-07

Issue #12544 - Add derived and external parameter attributes

Issue #12886 - Fix missing external parameters

Issue #12828 - Prevent time parameter from being filtered so aggregation works

Issue #11543 - Add annotations to NetCDF and CSV requests

Issue #12796 - Fix incorrect provenance source for interpolated parameters

Issue #12323 - Updated the preload_database pointer with the zplsc echogram changes.

# Release 1.3.8 2017-09-29

Issue #11444 - Prevent crash on open ended annotations

Issue #12318 - Filter NetCDF by selected parameters

Issue #12072 - enforce order on XDeployment JSON fields for readability

# Release 1.3.7 2017-07-20

Issue #12238 - Don't drop duplicate timestamps from separate deployments

Issue #12456 - Source in conda engine environment when running via script

# Release 1.3.6

Issue #9306 - Provide depth source for fixed ADCP instruments

# Release 1.3.5

Issue #12215 - Sampling strategy returns data outside query bounds

# Release 1.3.4

Update preload database to 1.0.12
Update xarray from 0.8.2 to 0.9.2
Handle unsigned int8 values in netcdf4 classic
Encode asset management last modified timestamp as string

# Release 1.3.3

Update preload database to 1.0.11
Update ion-functions to 2.3.1

# Release 1.3.2

Update preload database to 1.0.10

# Release 1.3.1

Fix issue with QC results and update numpy
Update preload database to 1.0.9

# Release 1.3.0

Updated required packages
- numpy
- numexpr
- scipy
- ooi-data
- flask
- gunicorn
- xarray
- pandas

# Release 1.2.10

Fix bugs preventing virtual stream computation
OOI Data version 0.0.5
Preload database version 1.0.8

# Release 1.2.9

Issue #12040 - parameter resolution error
Issue #10396 - parameter resolution error
Issue #4107 - dissolved oxygen computation fixes
Issue #9011 - dissolved oxygen computation fixes

# Release 1.2.8

Issue #12035
- Update resolution of L2 parameters that depend on other L2

# Release 1.2.7

Migrate preload data model to ooi_data

# Release 1.2.6

Issue #10777
- Don't report all data masked when querying virtual streams

# Release 1.2.5

Issue #11871
- Don't consider out-of-range bins when computing particle count estimate

Issue #11783 Add QC Flag to annotations
- Updated AnnotationRecord to include QCFlag

# Release 1.2.4

Issue #11952
- Updated aggregation response to be JSON (was plain string)

Issue #11861
- QC executed and QC results variables are incorrectly typed

Issue #10777
- modify stream_engine to utilize deployment times to bound product generation requests

# Release 1.2.3

Issue #11753
- Aggregation fails when string lengths differ between datasets

Issue #11715
- Added source attribute to the AnnotationRecord class to identify the creator of the data.

# Release 1.2.2

Issue #11646
- Production generation fails if no asset management data exists for one or more deployments

Issue #11642
- Bumped preload database pointer (v1.0.1) for "bin sizes incorrect in preload database"

# Release 1.2.1

Issue #11371
- Updated NetCDF header sensor asset management information

# Release 1.2.0

Issue #11371
- Add sensor asset management information to NetCDF header

# Release 1.1.2

Issue #11624
- Aggregation fails for CSV files

Issue #11623
- Aggregation fails if the aggregation host does not have any local files

Issue #11613
- Fixed hard-coded log level in util/gather.py

# Release 1.1.1

Issue #11528
- Accept the "execute_dpa" flag. When set to false no data product algorithms will be executed.

Issue #10820
- Stream engine will create a status.txt file containing the string "complete" when aggregation is complete.

Updated preload database pointer

# Release 1.1.0

Issue #10820 - NetCDF aggregation
- Stream engine will now aggregate the output from subjobs into one or more larger files

# Release 1.0.3

Issue #11497 - Incorrectly building 2D string arrays
- Fixed an issue which caused some queries to abort when the requested stream contains string arrays

# Release 1.0.2

Issue #11499 - Simplify overriding configuration values in Stream Engine
- Moved default configuration from config.py to config/default.py
- Created empty config/local.py to contain local config changes

Issue #11439 - Race condition in directory creation
- Fixed a potential race condition in directory creation

Story #1007 - 1.3.6.9.2 Cassandra Fault Resolution: Stream Engine Manager: code to manage product request and sub-request allocation to stream_engines
- Added /estimate endpoint to return file size and request time estimates.
