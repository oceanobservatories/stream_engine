import global_test_setup

import json
import unittest


from preload_database.database import initialize_connection, open_connection, PreloadDatabaseMode
from preload_database.model.preload import Parameter
from util.jsonresponse import NumpyJSONEncoder
from util.provenance_metadata_store import ProvenanceMetadataStore


initialize_connection(PreloadDatabaseMode.POPULATED_MEMORY)
open_connection()


class ProvenanceMetadataTest(unittest.TestCase):
    def test_unknown_function(self):
        store = ProvenanceMetadataStore(None)
        parameter = Parameter.query.get(911)
        to_attach = {'type': 'UnknownFunctionError',
                     "parameter": parameter,
                     'function': parameter.parameter_function.function_type}
        store.calculated_metadata.errors.append(to_attach)
        with self.assertRaises(TypeError):
            json.dumps(store.get_json())
        self.assertTrue(json.dumps(store.get_json(), cls=NumpyJSONEncoder))
