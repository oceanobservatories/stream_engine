import uuid
from collections import defaultdict


class CalculatedProvenanceMetadataStore(object):
    """Metadata store for provenance values"""

    def __init__(self):
        self.params = defaultdict(list)
        self.calls = {}
        self.ref_map = defaultdict(list)
        self.errors = []

    def insert_metadata(self, parameter, to_insert):
        # check to see if we have a matching metadata call
        # if we do return that id otherwise store it.
        for call in self.params[parameter]:
            if dict_equal(to_insert, self.calls[call]):
                return call
        # create and id and append it to the list
        call_id = str(uuid.uuid4())
        self.calls[call_id] = to_insert
        self.params[parameter].append(call_id)
        self.ref_map[parameter.id].append(call_id)
        return call_id

    def get_dict(self):
        """return dictionary representation"""
        res = {'parameters': {parameter.name: v for parameter, v in self.params.iteritems()},
               'calculations': self.calls, 'errors': self.errors}
        return res

    def get_keys_for_calculated(self, parameter):
        return self.ref_map[parameter]


def dict_equal(d1, d2):
    """Function to recursively check if two dicts are equal"""
    if isinstance(d1, dict) and isinstance(d2, dict):
        # check keysets
        if set(d1) != set(d2):
            return False

        # otherwise loop through all the keys and check if the dicts and items are equal
        for key in d1:
            if key in d2:
                if not dict_equal(d1[key], d2[key]):
                    return False
        # we made it through
        return True
    # check equality on other objects
    else:
        return d1 == d2