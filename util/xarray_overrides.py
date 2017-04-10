import warnings

import numpy as np
import pandas as pd

import xarray.backends.api
import xarray as xr

from xarray.conventions import (CharToStringArray, NativeEndiannessArray, DecodedCFTimedeltaArray, pop_to,
                                DecodedCFDatetimeArray, TIME_UNITS, MaskedAndScaledArray, BoolTypeArray)
from xarray.core import indexing, utils
from xarray.core.variable import as_variable, Variable


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


xarray.conventions.decode_cf_variable = decode_cf_variable

__all__ = [xr]
