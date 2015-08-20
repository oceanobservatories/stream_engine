import numpy as np
import xray
import util.xray_interpolation as xinterp

# Test interpolation outside range on both sides, between a value, and on a value
time = [1,3,4,7,9]
interp_time = [0,2,3,10]

# Same thing but for second dimension
var_dim_0 = [0,1,2]
interp_var_dim_0 = [-1,1,1.5,3]

# Test that attrs are preserved
attrs = {'someattr1': 'somevalue'}

# Test multiple types, 1D and nD arrays, and with and without fill
boolV = xray.DataArray([True, False, True, False, True], coords=[time], dims=['time'], attrs=attrs)
boolV_interp = xray.DataArray([True,True,False,True], coords=[interp_time], dims=['time'], attrs=attrs)

intV = xray.DataArray([1,2,3,4,5], coords=[time], dims=['time'])
intV_interp = xray.DataArray([1,1.5,2,5], coords=[interp_time], dims=['time'])
intV_interp_lastseen = xray.DataArray([1,1,2,5], coords=[interp_time], dims=['time'])

intV_fill = xray.DataArray([1,2,3,4,5], coords=[time], dims=['time'], attrs={'_FillValue': 2})
intV_interp_fill = xray.DataArray([1,np.NaN,np.NaN,5], coords=[interp_time], dims=['time'], attrs={'_FillValue': np.NaN})

stringV = xray.DataArray(['str1', 'str2','str3','str4', 'str5'], coords=[time], dims=['time'])
stringV_interp = xray.DataArray(['str1','str1','str2','str5'], coords=[interp_time], dims=['time'])

multiStringV = \
    xray.DataArray([['s1', 's2', 's3'],
                    ['s3', 's4', 's5'],
                    ['s6', 's7', 's8'],
                    ['s9', 's10', 's11'],
                    ['s12', 's13', 's14']],
                   coords=[time, var_dim_0],
                   dims=['time', 'var_dim_0'])

multiStringV_interp_var_dim_0 = \
    xray.DataArray([['s1', 's2', 's2', 's3'],
                    ['s3', 's4', 's4', 's5'],
                    ['s6', 's7', 's7', 's8'],
                    ['s9', 's10', 's10', 's11'],
                    ['s12', 's13', 's13', 's14']],
                   coords=[time, interp_var_dim_0],
                   dims=['time', 'var_dim_0'])

multiV = \
    xray.DataArray([[1, 2, 3],
                    [3, 4, 5],
                    [6, 7, 8],
                    [9, 10, 11],
                    [12, 13, 14]],
                   coords=[time, var_dim_0],
                   dims=['time', 'var_dim_0'])

multiV_interp = \
    xray.DataArray([[1, 2, 3],
                    [2, 3, 4],
                    [3, 4, 5],
                    [12, 13, 14]],
                   coords=[interp_time, var_dim_0],
                   dims=['time', 'var_dim_0'])

multiV_interp_var_dim_0 = \
    xray.DataArray([[1, 2, 2.5, 3],
                    [3, 4, 4.5, 5],
                    [6, 7, 7.5, 8],
                    [9, 10, 10.5, 11],
                    [12, 13, 13.5, 14]],
                   coords=[time, interp_var_dim_0],
                   dims=['time', 'var_dim_0'])

def test_bool_dtype():
    assert xinterp.interp1d_DataArray(boolV, time=interp_time).identical(boolV_interp)

def test_int_dtype():
    assert xinterp.interp1d_DataArray(intV, time=interp_time).identical(intV_interp)

def test_string_dtype():
    assert xinterp.interp1d_DataArray(stringV, time=interp_time).identical(stringV_interp)

def test_nD_array():
    assert xinterp.interp1d_DataArray(multiV, time=interp_time).identical(multiV_interp)
    assert xinterp.interp1d_DataArray(multiV, var_dim_0=interp_var_dim_0).identical(multiV_interp_var_dim_0)
    assert xinterp.interp1d_DataArray(multiStringV, var_dim_0=interp_var_dim_0).identical(multiStringV_interp_var_dim_0)

def test_dataset():
    ds = xray.Dataset({'boolV': boolV, 'intV': intV, 'stringV': stringV, 'multiV': multiV}, attrs=attrs)
    ds_interp = xray.Dataset({'boolV': boolV_interp, 'intV': intV_interp, 'stringV': stringV_interp, 'multiV': multiV_interp}, attrs=attrs)
    assert xinterp.interp1d_Dataset(ds, time=interp_time).identical(ds_interp)

def test_dataset_method():
    ds = xray.Dataset({'boolV': boolV, 'intV': intV, 'stringV': stringV, 'multiV': multiV}, attrs=attrs)
    ds_interp = xray.Dataset({'boolV': boolV_interp, 'intV': intV_interp_lastseen, 'stringV': stringV_interp, 'multiV': multiV_interp}, attrs=attrs)
    assert xinterp.interp1d_Dataset(ds, method={'intV': 'lastseen'}, time=interp_time).identical(ds_interp)

def test_fill():
    assert xinterp.interp1d_DataArray(intV_fill, time=interp_time).identical(intV_interp_fill)

