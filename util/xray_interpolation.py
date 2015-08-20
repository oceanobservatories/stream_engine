import numpy as np
import scipy.interpolate as interpolate
import xray

def interp1d_DataArray(data, **indexers):
    """
    Conform this object onto a new set of indexes, interpolating missing values.
    Numeric data is linearly interpolated if the interpolation dimension is
    the last dimension in the data.  This will result in data that has a dtype
    of float.  All other cases will interpolate the data as last seen.

    :param data: DataArray
    :param indexers: Keyword arguments.  Dictionary with keys given by
    dimension names and values given by arrays of coordinates tick labels.  Only
    one is supported right now.  If more than one is given, a random one will be
    choosen.
    :return: new interpolated DataArray with dimensions and attributes
    copied from data.

    Example
    >>> xinterp.interp1d_DataArray(da, time=[2,4,6])
    """
    dim_name = indexers.iterkeys().next()
    axis = data.dims.index(dim_name)
    coords = indexers[dim_name]

    x = data.coords[dim_name].values
    y = data.values

    if coords[0] < x[0]:
        x = np.insert(x, 0, coords[0])
        y = np.insert(y, 0, np.take(y, 0, axis=axis), axis=axis)

    if coords[-1] > x[-1]:
        x = np.append(x, [coords[-1]])
        y = np.append(y, np.take(y, [-1], axis=axis), axis=axis)

    if y.ndim == (axis+1) and y.dtype.kind in ['i','u','f','c']:
        # if 1D numeric then interpolate directly
        # note that the returned dtype will be float
        f = interpolate.interp1d(x, y, kind='linear', axis=axis, copy=False)
        interpdata = f(coords)
    else:
        # if nD or not numeric then interpolate and truncate to last seen integer index
        f = interpolate.interp1d(x, range(len(x)), kind='linear', axis=axis, copy=False)
        i = np.floor(f(coords)).astype(int)
        interpdata = np.take(y, i, axis=axis)

    interpcoords = dict(data.coords)
    interpcoords[dim_name] = coords
    return xray.DataArray(interpdata, coords=interpcoords, dims=data.dims, attrs=data.attrs)

def interp1d_Dataset(data, **kw_indexers):
    """
    Conform this object onto a new set of indexes, interpolating missing values.
    Numeric data is linearly interpolated if the interpolation dimension is
    the last dimension in the data.  This will result in data that has a dtype
    of float.  All other cases will interpolate the data as last seen.

    :param data: Dataset
    :param kw_indexers: Keyword arguments.  Dictionary with keys given by
    dimension names and values given by arrays of coordinates tick labels.  Only
    one is supported right now.  If more than one is given, a random one will be
    choosen.
    :return: Copy of dataset, with coordinates replaced and data interpolated.

    Example
    >>> xinterp.interp1d_Dataset(ds, time=[2,4,6])
    """
    index_name = kw_indexers.iterkeys().next()
    ds = data.drop(index_name)
    for i in data:
        if i != index_name and index_name in data[i].dims:
            ds[i] = interp1d_DataArray(data[i], **kw_indexers)
    return ds