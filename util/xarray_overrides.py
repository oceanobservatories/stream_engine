import warnings

import numpy as np
import pandas as pd

import xarray.backends.api
import xarray as xr

from xarray.conventions import (CharToStringArray, NativeEndiannessArray, DecodedCFTimedeltaArray, pop_to,
                                DecodedCFDatetimeArray, TIME_UNITS, MaskedAndScaledArray, BoolTypeArray)
from xarray.core.dataset import Dataset, as_dataset
from xarray.core.dataarray import DataArray
from xarray.core import indexing, utils
from xarray.core.pycompat import iteritems, OrderedDict
from xarray.core.variable import as_variable, Variable, concat as concat_vars
from xarray.core.combine import _calc_concat_dim_coord, _calc_concat_over
from xarray.core.alignment import align


# PATCH default XARRAY behavior when restoring string variables from netCDF
# See https://github.com/pydata/xarray/issues/1193
# REMOVE when corrected upstream
def decode_cf_variable(var, concat_characters=True, mask_and_scale=True,
                       decode_times=True, decode_endianness=True):
    """
    Decodes a variable which may hold CF encoded information.

    This includes variables that have been masked and scaled, which
    hold CF style time variables (this is almost always the case if
    the dataset has been serialized) and which have strings encoded
    as character arrays.

    Parameters
    ----------
    var : Variable
        A variable holding potentially CF encoded information.
    concat_characters : bool
        Should character arrays be concatenated to strings, for
        example: ['h', 'e', 'l', 'l', 'o'] -> 'hello'
    mask_and_scale: bool
        Lazily scale (using scale_factor and add_offset) and mask
        (using _FillValue).
    decode_times : bool
        Decode cf times ('hours since 2000-01-01') to np.datetime64.
    decode_endianness : bool
        Decode arrays from non-native to native endianness.

    Returns
    -------
    out : Variable
        A variable holding the decoded equivalent of var
    """
    # use _data instead of data so as not to trigger loading data
    var = as_variable(var)
    data = var._data
    dimensions = var.dims
    attributes = var.attrs.copy()
    encoding = var.encoding.copy()

    original_dtype = data.dtype

    if concat_characters:
        if data.dtype.kind == 'S' and data.dtype.itemsize == 1 and data.shape[-1] != 0:
            dimensions = dimensions[:-1]
            data = CharToStringArray(data)

    if mask_and_scale:
        if 'missing_value' in attributes:
            # missing_value is deprecated, but we still want to support it as
            # an alias for _FillValue.
            if ('_FillValue' in attributes and
                not utils.equivalent(attributes['_FillValue'],
                                     attributes['missing_value'])):
                raise ValueError("Discovered conflicting _FillValue "
                                 "and missing_value.  Considering "
                                 "opening the offending dataset using "
                                 "decode_cf=False, corrected the attributes",
                                 "and decoding explicitly using "
                                 "xarray.conventions.decode_cf(ds)")
            attributes['_FillValue'] = attributes.pop('missing_value')

        fill_value = np.array(pop_to(attributes, encoding, '_FillValue'))
        if fill_value.size > 1:
            warnings.warn("variable has multiple fill values {0}, decoding "
                          "all values to NaN.".format(str(fill_value)),
                          RuntimeWarning, stacklevel=3)
        scale_factor = pop_to(attributes, encoding, 'scale_factor')
        add_offset = pop_to(attributes, encoding, 'add_offset')
        if ((fill_value is not None and not np.any(pd.isnull(fill_value))) or
                scale_factor is not None or add_offset is not None):
            if fill_value.dtype.kind in ['U', 'S']:
                dtype = object
            else:
                dtype = float
            data = MaskedAndScaledArray(data, fill_value, scale_factor,
                                        add_offset, dtype)

    if decode_times and 'units' in attributes:
        if 'since' in attributes['units']:
            # datetime
            units = pop_to(attributes, encoding, 'units')
            calendar = pop_to(attributes, encoding, 'calendar')
            data = DecodedCFDatetimeArray(data, units, calendar)
        elif attributes['units'] in TIME_UNITS:
            # timedelta
            units = pop_to(attributes, encoding, 'units')
            data = DecodedCFTimedeltaArray(data, units)

    if decode_endianness and not data.dtype.isnative:
        # do this last, so it's only done if we didn't already unmask/scale
        data = NativeEndiannessArray(data)
        original_dtype = data.dtype

    if 'dtype' in encoding:
        if original_dtype != encoding['dtype']:
            warnings.warn("CF decoding is overwriting dtype")
    else:
        encoding['dtype'] = original_dtype

    if 'dtype' in attributes and attributes['dtype'] == 'bool':
        del attributes['dtype']
        data = BoolTypeArray(data)

    return Variable(dimensions, indexing.LazilyIndexedArray(data),
                    attributes, encoding=encoding)


def drop(self, labels, dim=None, inplace=False):
    """Drop variables or index labels from this dataset. Based on xarray.dataset.drop, but adds inplace option.

    Parameters
    ----------
    labels : scalar or list of scalars
        Name(s) of variables or index labels to drop.
    dim : None or str, optional
        Dimension along which to drop index labels. By default (if
        ``dim is None``), drops variables rather than index labels.
    inplace : whether the original dataset should be modified or a new one created

    Returns
    -------
    dropped : Dataset (self if inplace=True)
    """
    if utils.is_scalar(labels):
        labels = [labels]
    if dim is None:
        self._assert_all_in_dataset(labels)
        drop = set(labels)
        variables = OrderedDict((k, v) for k, v in iteritems(self._variables)
                                if k not in drop)
        coord_names = set(k for k in self._coord_names if k in variables)
        result = self._replace_vars_and_dims(variables, coord_names, inplace=inplace)
    else:
        try:
            index = self.indexes[dim]
        except KeyError:
            raise ValueError(
                'dimension %r does not have coordinate labels' % dim)
        new_index = index.drop(labels)
        result = self.loc[{dim: new_index}]
        
    return self if inplace else result


def _dataset_concat_coord_patch(datasets, dim, data_vars, coords, compat, positions):
    """
    Concatenate a sequence of datasets along a new or existing dimension
    This version is based on xarray.core.combine._dataset_concat but it will drop mismatched coordinates (remove them
    from coordinates but keep the variable) instead of throwing a ValueError, it adds logic to fix a bug wherein the 1st
    dataset is not checked for consistency with the others, and the import statements were removed to the module level.
    """
    if compat not in ['equals', 'identical']:
        raise ValueError("compat=%r invalid: must be 'equals' "
                         "or 'identical'" % compat)

    dim, coord = _calc_concat_dim_coord(dim)
    datasets = [as_dataset(ds) for ds in datasets]
    datasets = align(*datasets, join='outer', copy=False, exclude=[dim])

    concat_over = _calc_concat_over(datasets, dim, data_vars, coords)

    def insert_result_variable(k, v):
        assert isinstance(v, Variable)
        if k in datasets[0].coords:
            result_coord_names.add(k)
        result_vars[k] = v

    # create the new dataset and add constant variables
    result_vars = OrderedDict()
    result_coord_names = set(datasets[0].coords)
    result_attrs = datasets[0].attrs

    for k, v in datasets[0].variables.items():
        if k not in concat_over:
            insert_result_variable(k, v)

    # check that global attributes and non-concatenated variables are fixed
    # across all datasets
    for ds in datasets[1:]:
        if (compat == 'identical' and
                not utils.dict_equiv(ds.attrs, result_attrs)):
            raise ValueError('dataset global attributes not equal')
        for k, v in iteritems(ds.variables):
            if k not in result_vars and k not in concat_over:
                raise ValueError('encountered unexpected variable %r' % k)
            elif (k in result_coord_names) != (k in ds.coords):
                # Instead of throwing a ValueError, make the coordinates match by removing the mismatched coordinate
                if k in result_coord_names:
                    datasets[0]._coord_names.remove(k)
                    result_coord_names.remove(k)
                else:  # k in ds.coords
                    ds._coord_names.remove(k)
            elif (k in result_vars and k != dim and
                  not getattr(v, compat)(result_vars[k])):
                verb = 'equal' if compat == 'equals' else compat
                raise ValueError(
                    'variable %r not %s across datasets' % (k, verb))

    # above for loop fails to check 1st dataset for consistency with the others (i.e. 1st dataset could have a
    # coordinate not present in the following datasets). The identical attrs check is fine.
    # Handle this check by comparing the 1st dataset to the 2nd with the 2nd as the control

    if len(datasets) > 1:
        ds = datasets[0]
        result_vars.clear()
        result_coord_names = set(datasets[1].coords)

        for k, v in datasets[1].variables.items():
            if k not in concat_over:
                insert_result_variable(k, v)

        for k, v in iteritems(ds.variables):
            if k not in result_vars and k not in concat_over:
                raise ValueError('encountered unexpected variable %r' % k)
            elif (k in result_coord_names) != (k in ds.coords):
                if k in result_coord_names:
                    datasets[1]._coord_names.remove(k)
                    result_coord_names.remove(k)
                else:  # k in ds.coords
                    ds._coord_names.remove(k)
            elif (k in result_vars and k != dim and
                  not getattr(v, compat)(result_vars[k])):
                verb = 'equal' if compat == 'equals' else compat
                raise ValueError(
                    'variable %r not %s across datasets' % (k, verb))

    # we've already verified everything is consistent; now, calculate
    # shared dimension sizes so we can expand the necessary variables
    dim_lengths = [ds.dims.get(dim, 1) for ds in datasets]
    non_concat_dims = {}
    for ds in datasets:
        non_concat_dims.update(ds.dims)
    non_concat_dims.pop(dim, None)

    def ensure_common_dims(vars):
        # ensure each variable with the given name shares the same
        # dimensions and the same shape for all of them except along the
        # concat dimension
        common_dims = tuple(pd.unique([d for v in vars for d in v.dims]))
        if dim not in common_dims:
            common_dims = (dim,) + common_dims
        for var, dim_len in zip(vars, dim_lengths):
            if var.dims != common_dims:
                common_shape = tuple(non_concat_dims.get(d, dim_len)
                                     for d in common_dims)
                var = var.expand_dims(common_dims, common_shape)
            yield var

    # stack up each variable to fill-out the dataset (in order)
    for k in datasets[0].variables:
        if k in concat_over:
            vars = ensure_common_dims([ds.variables[k] for ds in datasets])
            combined = concat_vars(vars, dim, positions)
            insert_result_variable(k, combined)

    result = Dataset(result_vars, attrs=result_attrs)
    result = result.set_coords(result_coord_names)

    if coord is not None:
        # add concat dimension last to ensure that its in the final Dataset
        result[coord.name] = coord

    return result


def _dataarray_concat_coord_patch(arrays, dim, data_vars, coords, compat,
                                  positions):
    """This version is identical to xarray.core.combine._dataarray_concat except that it calls a patched version of
    _dataset_concat in order to handle coordinate mismatches."""
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

    ds = _dataset_concat_coord_patch(datasets, dim, data_vars, coords, compat,
                         positions)
    return arrays[0]._from_temp_dataset(ds, name)


def concat_coord_patch(objs, dim=None, data_vars='all', coords='different',
                       compat='equals', positions=None):
    """Concatenate xarray objects along a new or existing dimension.
    This version is based on xarray.core.combine.concat but calls patched versions of _dataset_concat and
    _dataarray_concat in order to handle coordinate mismatches.

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
        f = _dataarray_concat_coord_patch
    elif isinstance(first_obj, Dataset):
        f = _dataset_concat_coord_patch
    else:
        raise TypeError('can only concatenate xarray Dataset and DataArray '
                        'objects, got %s' % type(first_obj))
    return f(objs, dim, data_vars, coords, compat, positions)


xarray.conventions.decode_cf_variable = decode_cf_variable
xarray.core.dataset.Dataset.drop = drop

__all__ = [xr]
