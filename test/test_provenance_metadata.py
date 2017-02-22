from ooi_data.postgres.model import MetadataBase

import global_test_setup

import json
import unittest


from preload_database.database import create_engine_from_url, create_scoped_session
from ooi_data.postgres.model import Parameter
from util.jsonresponse import NumpyJSONEncoder
from util.provenance_metadata_store import ProvenanceMetadataStore


engine = create_engine_from_url(None)
session = create_scoped_session(engine)
MetadataBase.query = session.query_property()


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
