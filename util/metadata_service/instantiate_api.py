import engine
from metadata_service_api import MetadataServiceAPI

metadata_service_api = MetadataServiceAPI(engine.app.config['STREAM_METADATA_SERVICE_URL'],
                                          engine.app.config['PARTITION_METADATA_SERVICE_URL'])
