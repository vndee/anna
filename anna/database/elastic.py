from anna.logger import get_logger
from anna.database.dbmeta import DatabaseMeta
from elasticsearch import Elasticsearch


logger = get_logger(__name__)

def elastic_verify(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        if 'index' not in kwargs or kwargs['index'] is None:
            logger.info(f'[{func.__name__}] Index can not be None.')
        try:
            rv = func(*args, **kwargs)
            if rv is not None:
                if ElasticsearchWrapper.LOG_LEVEL == 1:
                    logger.info(f'[{func.__name__}] Healthy response.')
                return rv
            else:
                return []
        except Exception as ex:
            logger.exception(ex)
            return []

    return wrapper


class ElasticsearchWrapper(metaclass=DatabaseMeta):
    LOG_LEVEL = 1

    def __init__(self, host=None, port=None, user=None, password=None, *args, **kwargs):
        assert host is not None, 'host can not be None.'
        assert port is not None, 'port can not be None.'

        self.__connector = Elasticsearch(host, 
                                        http_auth=(user, password), 
                                        scheme='http', 
                                        port=port, 
                                        http_compress=True, 
                                        verify_certs=False)

    @elastic_verify
    def insert(self, index=None, body=None, *args, **kwargs):
        assert body is not None, '[insert] body can not be None.'
        return self.__connector.index(index=index, body=body)

    @elastic_verify
    def delete(self, index=None, _id=None, *args, **kwargs):
        assert _id is not None, '[delete] _id can not be None.'
        return self.__connector.delete(index=index, id=_id)
    
    @elastic_verify
    def update(self, index=None, _id=None, body=None, *args, **kwargs):
        assert _id is not None, '[update] _id can not be None.'
        assert body is not None, '[update] body can not be None.'
        return self.__connector.update(index=index, id=_id, body=body)

    @elastic_verify
    def get(self, index=None, _id=None, *args, **kwargs):
        assert _id is not None, '[get] _id can not be None.'
        return self.__connector.get(index=index, id=_id)

    @elastic_verify
    def create_index(self, index=None, *args, **kwargs):
        return self.__connector.create(index=index)

    @elastic_verify
    def delete_index(self, index=None, *args, **kwargs):
        return self.__connector.delete(index=index)

    @elastic_verify
    def is_index_exists(self, index=None, *args, **kwargs):
        if self.__connector.indices.exists(index=index):
            return True
        else:
            return False

    @elastic_verify
    def search_topic(self, index=None, query=None, *args, **kwargs):
        if query is None:
            logger.info('[search] query is None.')
            return []

        results = self.__connector.search(index=index, body=query)['hits']['hits']
        return [result['_source'] for result in results]
