import warnings

import numpy as np
import pandas as pd

import xarray.backends.api
import xarray as xr
from xarray.core import indexing, utils
from collections import OrderedDict
from xarray.core.variable import as_variable, Variable


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
        variables = OrderedDict((k, v) for k, v in self._variables.items()
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


xarray.core.dataset.Dataset.drop = drop

__all__ = [xr]
