from xarray.backends import api, NetCDF4DataStore
from xarray import open_dataset

class NetCDF4DataStoreUnlimited(NetCDF4DataStore):
    # Override set_necessary_dimensions to allow the 
    # dimension defined in 'unlimited' list to be set 
    # to unlimited
    def set_necessary_dimensions(self, variable):
        unlimited_dim = variable.encoding.pop('unlimited', [])
        for d, l in zip(variable.dims, variable.shape):
            if d in unlimited_dim:
                l = None
            if d not in self.dimensions:
                self.set_dimension(d, l)

