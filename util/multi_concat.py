import warnings

import numpy as np
import pandas as pd

import xarray.backends.api
import xarray as xr

from xarray.core.dataset import Dataset
from xarray.core.dataarray import DataArray
from xarray.core import utils
from xarray.core.pycompat import iteritems, OrderedDict
from xarray.core.variable import Variable, IndexVariable, as_variable, concat as concat_vars
from xarray.core.alignment import align


try:
    # solely for isinstance checks
    import dask.array as dask_array

    dask_array_type = (dask_array.Array,)
except ImportError:  # pragma: no cover
    dask_array_type = ()
    dask_array = None


def asarray(data):
    return (
        data
        if (isinstance(data, dask_array_type) or hasattr(data, "__array_function__"))
        else np.asarray(data)
    )


def lazy_array_equiv(arr1, arr2):
    """Like array_equal, but doesn't actually compare values.
       Returns True when arr1, arr2 identical or their dask names are equal.
       Returns False when shapes are not equal.
       Returns None when equality cannot determined: one or both of arr1, arr2 are numpy arrays;
       or their dask names are not equal
    """
    if arr1 is arr2:
        return True
    arr1 = asarray(arr1)
    arr2 = asarray(arr2)
    if arr1.shape != arr2.shape:
        return False
    if (
        dask_array
        and isinstance(arr1, dask_array.Array)
        and isinstance(arr2, dask_array.Array)
    ):
        # GH3068
        if arr1.name == arr2.name:
            return True
        else:
            return None
    return None


def _parse_datasets(datasets):
    """
    Determine dimensional coordinate names and a dict mapping name to DataArray. This function has been copied as-is
    from xarray.core.concat.py in xarray version 0.15. It should be imported when we update to a version of xarray
    that includes it.
    """
    dims = set()
    all_coord_names = set()
    data_vars = set()  # list of data_vars
    dim_coords = {}  # maps dim name to variable
    dims_sizes = {}  # shared dimension sizes to expand variables

    for ds in datasets:
        dims_sizes.update(ds.dims)
        all_coord_names.update(ds.coords)
        data_vars.update(ds.data_vars)

        for dim in set(ds.dims) - dims:
            if dim not in dim_coords:
                dim_coords[dim] = ds.coords[dim].variable
        dims = dims | set(ds.dims)

    return dim_coords, dims_sizes, all_coord_names, data_vars


# updated to support multiple dims instead of one by using lists instead of just returning dim and coord
def _calc_concat_dims_coords(dims):
    """
    Infer the dimension names and 1d coordinate variables (if appropriate)
    for concatenating along the new dimensions. Based on the function _calc_concat_dim_coord in xarray, but updated to
    support multiple dims.
    """
    dimensions = []
    coordinates = []
    
    for dim in dims:
        if isinstance(dim, basestring):
            coord = None
        elif not hasattr(dim, 'dims'):
            # dim is not a DataArray or IndexVariable
            dim_name = getattr(dim, "name", None)
            if dim_name is None:
                dim_name = "concat_dim"
            coord = IndexVariable(dim_name, dim)
            dim = dim_name
        elif not hasattr(dim, 'name'):
            coord = as_variable(dim).to_index_variable()
            (dim,) = coord.dims
        else:
             coord = dim
             (dim,) = coord.dims

        dimensions.append(dim)
        coordinates.append(coord)

    return dimensions, coordinates


# updated to handle multiple dims, adding to concat_over for each and using a dictionary for dim lengths
# process_subset_opt logic unaffected
def _calc_concat_over(datasets, dims, dim_names, data_vars, coords, compat):
    """
    Determine which dataset variables need to be concatenated in the result,
    """
    # Return values
    concat_over = set()
    equals = {}
    concat_dim_lengths = {}

    for dim in dims:
        if dim in dim_names:
            concat_over_existing_dim = True
            concat_over.add(dim)
        else:
            concat_over_existing_dim = False
    
        for ds in datasets:
            # make sure dim is a coordinate in each dataset
            if concat_over_existing_dim:
                if dim not in ds.dims:
                    if dim in ds:
                        ds = ds.set_coords(dim)
            concat_over.update(k for k, v in ds.variables.items() if dim in v.dims)
            
            # don't concat along non obs dimensions (i.e. wavelength) containing duplicate data, this can lead to errors
            if dim != 'obs' and len(datasets) > 1 and dim in concat_dim_lengths.keys():
                ds_non_obs_vars = [var for var in ds.data_vars if 'obs' not in ds[var].dims]
                first_non_obs_vars = datasets[0][ds_non_obs_vars].data_vars
                for key in first_non_obs_vars.keys():
                    #if data in non obs dimension is not duplicate (compared to first dataset) update concat_dim_lengths
                    if not ds[key].equals(first_non_obs_vars[key]):
                        # insert dim length for each ds into concat_dim_lengths under appropriate key
                        concat_dim_lengths.setdefault(dim, []).append(ds.dims.get(dim, 1))
            else:
                # insert dim length for each ds into concat_dim_lengths under appropriate key
                concat_dim_lengths.setdefault(dim, []).append(ds.dims.get(dim, 1))

    def process_subset_opt(opt, subset):
        if isinstance(opt, str):
            if opt == "different":
                if compat == "override":
                    raise ValueError(
                        "Cannot specify both %s='different' and compat='override'."
                        % subset
                    )
                # all nonindexes that are not the same in each dataset
                for k in getattr(datasets[0], subset):
                    if k not in concat_over:
                        equals[k] = None
                        variables = [ds.variables[k] for ds in datasets]
                        # first check without comparing values i.e. no computes
                        for var in variables[1:]:
                            equals[k] = getattr(variables[0], compat)(
                                var, equiv=lazy_array_equiv
                            )
                            if equals[k] is not True:
                                # exit early if we know these are not equal or that
                                # equality cannot be determined i.e. one or all of
                                # the variables wraps a numpy array
                                break

                        if equals[k] is False:
                            concat_over.add(k)

                        elif equals[k] is None:
                            # Compare the variable of all datasets vs. the one
                            # of the first dataset. Perform the minimum amount of
                            # loads in order to avoid multiple loads from disk
                            # while keeping the RAM footprint low.
                            v_lhs = datasets[0].variables[k].load()
                            # We'll need to know later on if variables are equal.
                            computed = []
                            for ds_rhs in datasets[1:]:
                                v_rhs = ds_rhs.variables[k].compute()
                                computed.append(v_rhs)
                                if not getattr(v_lhs, compat)(v_rhs):
                                    concat_over.add(k)
                                    equals[k] = False
                                    # computed variables are not to be re-computed
                                    # again in the future
                                    for ds, v in zip(datasets[1:], computed):
                                        ds.variables[k].data = v.data
                                    break
                            else:
                                equals[k] = True

            elif opt == "all":
                concat_over.update(
                    set(getattr(datasets[0], subset)) - set(datasets[0].dims)
                )
            elif opt == "minimal":
                pass
            else:
                raise ValueError("unexpected value for {subset}: {opt}")
        else:
            invalid_vars = [k for k in opt if k not in getattr(datasets[0], subset)]
            if invalid_vars:
                if subset == "coords":
                    raise ValueError(
                        "some variables in coords are not coordinates on "
                        "the first dataset: %s" % (invalid_vars,)
                    )
                else:
                    raise ValueError(
                        "some variables in data_vars are not data variables "
                        "on the first dataset: %s" % (invalid_vars,)
                    )
            concat_over.update(opt)

    process_subset_opt(data_vars, "data_vars")
    process_subset_opt(coords, "coords")
    return concat_over, equals, concat_dim_lengths


def _get_concat_dim(dims, vars):
    """
    Helper method to search for a dimension along which to concatenate. It looks at the common dimensions among vars 
    and searches in order for each dimension in dims. In this way, dims acts as a list of dimensions to use for 
    concatenation in order of preference.
    """
    # find the first concat dimension available in vars
    common_dims = tuple(pd.unique([d for v in vars for d in v.dims]))
    for dim in dims:
       if dim in common_dims:
           return dim
    # in the event none of the supplied dimensions are available, assume the most preferred dimension will be added to
    # the variables for concatenation
    return dims[0]


def _find_concat_dims(datasets, dim):
    """
    Helper method to generate a list of alternative dimensions along which Dataset variables may be concatenated. This
    allows different variables to be concatenated along different dimensions, and does so without the caller needing to
    analyze the particular data to identify suitable alternatives. The dimension passed in dim is the preferred
    dimension and will be the one concatenated over in all cases where said dimension is already present. In cases
    where the preferred dimension is absent, whatever dimensions are present will be added to the alternatives list.
    """ 
    dims = set()
    for name, var in datasets[0].variables.iteritems():
        # Do NOT add dims from variables that already have the preferred dim available - we are only interested in 
        # alternate dims to concatenate on when the preferred dim is absent
        # Do NOT add a dim for concatenation over when the only variables that would be concatenated on it are dimensions!
        if dim not in var.dims and name not in datasets[0].dims:
            dims.update(var.dims)
    # we used a set for dims to prevent duplicates - now convert to a list to have a defined order
    dims = list(dims)
    # put dim at the front of the list as the most preferred dim for concatenation
    dims.insert(0, dim)
    return dims 


def _dataset_multi_concat(
    datasets,
    dim,
    data_vars,
    coords,
    compat,
    positions,
    join="outer",
):
    """
    Concatenate a sequence of datasets along a dimension, trying concatenation along alternate dimensions when the 
    chosen dimension is not present. This function is based on _dataset_concat from xarray.core.concat.py in xarray 
    0.15. It includes a modification to drop mismatched coordinates from datasets instead of throwing a ValueError. 
    This drop removes the variable from coordinates, but it remains a variable in the dataset.
    """
    # Make sure we're working on a copy (we'll be loading variables)
    datasets = [ds.copy() for ds in datasets]
    
    # determine what dimensions we will be concatenating over, including the preferred dim and any alternatives when
    # the preferred dim is absent
    dims = _find_concat_dims(datasets, dim)
    dims, coordinates = _calc_concat_dims_coords(dims)
    
    datasets = align(
        *datasets, join=join, copy=False, exclude=dims
    )
    
    dim_coords, dims_sizes, coord_names, data_names = _parse_datasets(datasets)
    dim_names = set(dim_coords)
    unlabeled_dims = dim_names - coord_names
    both_data_and_coords = coord_names & data_names
    if both_data_and_coords:
        # Instead of throwing a ValueError, make the coordinates match by removing the mismatched coordinate
        for ds in datasets:
            for variable in both_data_and_coords:
                if variable in ds.coords:
                    # This makes the variable no longer a coordinate, but does not remove it from the dataset entirely
                    ds._coord_names.remove(variable)
                    coord_names.discard(variable)
    
    # we don't want the concat dimensions in the result dataset yet
    for dim in dims:
        dim_coords.pop(dim, None)
        dims_sizes.pop(dim, None)

        # case where concat dimension is a coordinate or data_var but not a dimension
        if (dim in coord_names or dim in data_names) and dim not in dim_names:
            datasets = [ds.expand_dims(dim) for ds in datasets]

    # determine which variables to concatenate
    concat_over, equals, concat_dim_lengths = _calc_concat_over(
        datasets, dims, dim_names, data_vars, coords, compat
    )

    # determine which variables to merge, and then merge them according to compat
    variables_to_merge = (coord_names | data_names) - concat_over - dim_names

    result_vars = {}
    if variables_to_merge:
        to_merge = {var: [] for var in variables_to_merge}

        for ds in datasets:
            for var in variables_to_merge:
                if var in ds:
                    to_merge[var].append(ds.variables[var])

        for var in variables_to_merge:
            result_vars[var] = unique_variable(
                var, to_merge[var], compat=compat, equals=equals.get(var, None)
            )
    else:
        result_vars = {}

    result_vars.update(dim_coords)

    # assign attrs and encoding from first dataset
    result_attrs = datasets[0].attrs
    result_encoding = datasets[0].encoding

    # check that global attributes are fixed across all datasets if necessary
    for ds in datasets[1:]:
        if compat == "identical" and not utils.dict_equiv(ds.attrs, result_attrs):
            raise ValueError("Dataset global attributes not equal.")

    # we've already verified everything is consistent; now, calculate
    # shared dimension sizes so we can expand the necessary variables
    def ensure_common_dims(vars):
        # ensure each variable with the given name shares the same
        # dimensions and the same shape for all of them except along the
        # concat dimension
        common_dims = tuple(pd.unique([d for v in vars for d in v.dims]))
        
        # find the first concat dimension available in vars
        concat_dim = [x for x in dims if x in common_dims][0]
        if not concat_dim:
            # none of the concat dims are present - add the first one
            dim = dims[0]
            common_dims = (dim,) + common_dims
            concat_dim = dim
        
        for var, dim_len in zip(vars, concat_dim_lengths[concat_dim]):
            if var.dims != common_dims:
                common_shape = tuple(dims_sizes.get(d, dim_len) for d in common_dims)
                var = var.expand_dims(common_dims, common_shape)
            yield var

    # stack up each variable to fill-out the dataset (in order)
    # n.b. this loop preserves variable order, needed for groupby.
    for k in datasets[0].variables:
        if k in concat_over:
            try:
                vars = ensure_common_dims([ds.variables[k] for ds in datasets])
            except KeyError:
                raise ValueError("%r is not present in all datasets." % k)
            # get the dimension to concatenate this variable on - choose first applicable dim from dims
            dim = _get_concat_dim(dims, [ds.variables[k] for ds in datasets])
            combined = concat_vars(vars, dim, positions)
            assert isinstance(combined, Variable)
            result_vars[k] = combined

    result = Dataset(result_vars, attrs=result_attrs)
    absent_coord_names = coord_names - set(result.variables)
    if absent_coord_names:
        raise ValueError(
            "Variables %r are coordinates in some datasets but not others."
            % absent_coord_names
        )
    # current versions of dataset.set_coords and dataset.drop force a _assert_all_in_dataset check that we don't want
    # xarray 0.15 has the option to disable this via errors='ignore', but for now just call the underlying logic
    #result = result.set_coords(coord_names, errors='ignore')
    result._coord_names.update(coord_names)
    result.encoding = result_encoding

    #result = result.drop(unlabeled_dims, errors='ignore')
    drop = set(unlabeled_dims)
    variables = OrderedDict((k, v) for k, v in iteritems(result._variables) if k not in drop)
    coord_names = set(k for k in result._coord_names if k in variables)
    result._replace_vars_and_dims(variables, coord_names)

    for coord in coordinates:
        if coord:
            # add concat dimension last to ensure that its in the final Dataset
            result[coord.name] = coord

    return result


def _dataarray_concat(arrays, dim, data_vars, coords, compat,
                                  positions):
    """This version is identical to xarray.core.combine._dataarray_concat except that it calls a patched version of
    _dataset_concat in order to handle coordinate mismatches and concatenation along non-obs dimensions."""
    arrays = list(arrays)

    if data_vars != 'all':
        raise ValueError('data_vars is not a valid argument when '
                         'concatenating DataArray objects')

    datasets = []
    for n, arr in enumerate(arrays):
        if n == 0:
            name = arr.name
        elif name != arr.name:
            if compat == 'identical':
                raise ValueError('array names not identical')
            else:
                arr = arr.rename(name)
        datasets.append(arr._to_temp_dataset())

    ds = _dataset_multi_concat(datasets, dim, data_vars, coords, compat,
                         positions)
    return arrays[0]._from_temp_dataset(ds, name)


def multi_concat(objs, dim=None, data_vars='all', coords='different',
                       compat='equals', positions=None):
    """Concatenate xarray objects along a new or existing dimension, considering alternative concatenation dimensions 
    when the given dimension is not present. This version is based on xarray.core.combine.concat but calls modified
    versions of _dataset_concat and _dataarray_concat in order to handle coordinate mismatches and non-obs dimensions.
    Parameters
    ----------
    objs : sequence of Dataset and DataArray objects
        xarray objects to concatenate together. Each object is expected to
        consist of variables and coordinates with matching shapes except for
        along the concatenated dimension.
    dim : str or DataArray or pandas.Index
        Name of the dimension to concatenate along. This can either be a new
        dimension name, in which case it is added along axis=0, or an existing
        dimension name, in which case the location of the dimension is
        unchanged. If dimension is provided as a DataArray or Index, its name
        is used as the dimension to concatenate along and the values are added
        as a coordinate.
    data_vars : {'minimal', 'different', 'all' or list of str}, optional
        These data variables will be concatenated together:
          * 'minimal': Only data variables in which the dimension already
            appears are included.
          * 'different': Data variables which are not equal (ignoring
            attributes) across all datasets are also concatenated (as well as
            all for which dimension already appears). Beware: this option may
            load the data payload of data variables into memory if they are not
            already loaded.
          * 'all': All data variables will be concatenated.
          * list of str: The listed data variables will be concatenated, in
            addition to the 'minimal' data variables.
        If objects are DataArrays, data_vars must be 'all'.
    coords : {'minimal', 'different', 'all' o list of str}, optional
        These coordinate variables will be concatenated together:
          * 'minimal': Only coordinates in which the dimension already appears
            are included.
          * 'different': Coordinates which are not equal (ignoring attributes)
            across all datasets are also concatenated (as well as all for which
            dimension already appears). Beware: this option may load the data
            payload of coordinate variables into memory if they are not already
            loaded.
          * 'all': All coordinate variables will be concatenated, except
            those corresponding to other dimensions.
          * list of str: The listed coordinate variables will be concatenated,
            in addition the 'minimal' coordinates.
    compat : {'equals', 'identical'}, optional
        String indicating how to compare non-concatenated variables and
        dataset global attributes for potential conflicts. 'equals' means
        that all variable values and dimensions must be the same;
        'identical' means that variable attributes and global attributes
        must also be equal.
    positions : None or list of integer arrays, optional
        List of integer arrays which specifies the integer positions to which
        to assign each dataset along the concatenated dimension. If not
        supplied, objects are concatenated in the provided order.

    Returns
    -------
    concatenated : type of objs

    See also
    --------
    merge
    auto_combine
    """
    # Below TODOs are copied from the xarray source code
    # TODO: add join and ignore_index arguments copied from pandas.concat
    # TODO: support concatenating scalar coordinates even if the concatenated
    # dimension already exists

    try:
        first_obj, objs = utils.peek_at(objs)
    except StopIteration:
        raise ValueError('must supply at least one object to concatenate')

    if dim is None:
        raise ValueError("Value of None is not allowed for argument 'dim'.")

    if isinstance(first_obj, DataArray):
        # using updated _dataarray_concat that calls _dataset_multi_concat instead of _dataset_concat
        f = _dataarray_concat
    elif isinstance(first_obj, Dataset):
        # call the modified _dataset_multi_concat instead of _dataset_concat
        f = _dataset_multi_concat
    else:
        raise TypeError('can only concatenate xarray Dataset and DataArray '
                        'objects, got %s' % type(first_obj))
    return f(objs, dim, data_vars, coords, compat, positions)
