import numpy as np
import scipy.interpolate as interpolate
import xray


def _fit(x, y, new_x, axis):
    if new_x[0] < x[0]:
        x = np.insert(x, 0, new_x[0])
        y = np.insert(y, 0, np.take(y, 0, axis=axis), axis=axis)
    if new_x[-1] > x[-1]:
        x = np.append(x, [new_x[-1]])
        y = np.append(y, np.take(y, [-1], axis=axis), axis=axis)
    return x, y


def _last_seen(x, y, new_x, axis):
    f = interpolate.interp1d(x, range(len(x)), kind='linear', axis=axis, copy=False)
    i = np.floor(f(new_x)).astype(int)
    return np.take(y, i, axis=axis)


def _numeric_interpolation(x, y, new_x, axis=0, fill=None):
    # Note that the returned dtype will be float.  If axis < ndim - 1 then
    # multidimensional data after axis will be element-wise interpolated.
    if fill is not None:
        y = np.where(y == fill, np.NaN, y)
    f = interpolate.interp1d(x, y, kind='linear', axis=axis, copy=False)
    return f(new_x)


def interp1d_DataArray(data, method=None, **indexers):
    """
    Conform this object onto a new set of indexes, interpolating missing values.
    Note that that returned dtype will be float if numberic data is linearly
    interpolated.  Attributes are copied.  If the special attribute _FillValue
    is set and numeric data is linearly interpolated, the _FillValue will be
    changed to NaN.  Fill values in the original data will be converted to NaN
    before being interpolated.

    :param data: DataArray
    :param method: Optional method of interpolation.  Default is to interpolate
    numeric data using linear interpolation, and other types using lastseen.  If
    'lastseen' is specified, all data will be interpolated as lastseen.  If
    'linear' is specified all data will be interpolated linearly.  This will
    raise an error if the data in not numeric.
    :param indexers: Keyword arguments.  Dictionary with keys given by
    dimension names and values given by arrays of coordinates tick labels.  Only
    one is supported right now.  If more than one is given, a random one will be
    choosen.
    :return: new interpolated DataArray with dimensions and attributes
    copied from data.
    :see xray.DataArray.reindex

    Example
    >>> xinterp.interp1d_DataArray(da, time=[2,4,6])
    """
    dim_name = indexers.iterkeys().next()
    axis = data.dims.index(dim_name)
    coords = indexers[dim_name]

    if method and method not in ['lastseen', 'linear']:
        raise ValueError('Unknown interpolation method')

    x = data.coords[dim_name].values
    y = data.values

    x,y = _fit(x, y, coords, axis)

    interpattrs = dict(data.attrs)
    new_fill_value = None
    if method != 'lastseen' and y.dtype.kind in ['i','u','f','c']:
        fill_value = interpattrs.get('_FillValue')
        interpdata = _numeric_interpolation(x, y, coords, axis, fill_value)
        if interpattrs.has_key('_FillValue'):
            interpattrs['_FillValue'] = np.NaN
    else:
        if method == 'linear':
            raise ValueError('Non-numeric data can\'t be linearly interpolated')
        interpdata = _last_seen(x, y, coords, axis)

    interpcoords = dict(data.coords)
    interpcoords[dim_name] = coords
    return xray.DataArray(interpdata, coords=interpcoords, dims=data.dims, attrs=interpattrs)

def interp1d_Dataset(data, method=None, **kw_indexers):
    """
    Conform this object onto a new set of indexes, interpolating missing values.
    Note that that returned dtype will be float if numberic data is linearly
    interpolated.  Attributes are copied.  If the special attribute _FillValue
    is set and numeric data is linearly interpolated, the _FillValue will be
    changed to NaN.  Fill values in the original data will be converted to NaN
    before being interpolated.

    :param data: Dataset
    :param method: Optional str or dict method of interpolation.  If dict, keys
    are variable to apply method and values are method. Default is to interpolate
    numeric data using linear interpolation, and other types using lastseen.  If
    'lastseen' is specified, all data will be interpolated as lastseen.  If
    'linear' is specified all data will be interpolated linearly.  This will
    raise an error if the data in not numeric.
    :param kw_indexers: Keyword arguments.  Dictionary with keys given by
    dimension names and values given by arrays of coordinates tick labels.  Only
    one is supported right now.  If more than one is given, a random one will be
    choosen.
    :return: Copy of dataset, with coordinates replaced and data interpolated.
    :see xray.Dataset.reindex

    Example
    >>> xinterp.interp1d_Dataset(ds, time=[2,4,6])
    """
    index_name = kw_indexers.iterkeys().next()
    ds = data.drop(index_name)
    for i in data:
        if i != index_name and index_name in data[i].dims:
            m = method.get(i) if(isinstance(method, dict)) else method
            ds[i] = interp1d_DataArray(data[i], method=m, **kw_indexers)
    return ds