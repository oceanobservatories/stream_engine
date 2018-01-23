# Stream Engine

# Development Release 1.4.1 2018-02-06

Issue #9328 - Make interpolated ctd pressure available
- add interpolated ctd pressure to the data request
- ensure interpolated ctd pressure is included in NetCDF

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

Story 1007 - 1.3.6.9.2 Cassandra Fault Resolution: Stream Engine Manager: code to manage product request and sub-request allocation to stream_engines
- Added /estimate endpoint to return file size and request time estimates.
