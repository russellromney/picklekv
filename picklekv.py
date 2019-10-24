from simplekv.db.sql import SQLAlchemyStore
import pickle
from sqlalchemy import select


class PickleKV(SQLAlchemyStore):
    '''
    extends the functionality of simplekv.db.SQLAlchemyStore to 
    allow saving of plain python objects by automatically pickling them

    params:
        bind - a sqlalchemy engine
        metadata - a sqlalchemy MetaData object
        tablename - str
        
    '''
    def __init__(self,bind,meta,tablename):
        super().__init__(bind,meta,tablename)

    def __setitem__(self,key,value):
        return self._put(key,value)

    def __getitem__(self,key):
        return self._get(key)

    def _get(self,key):
        rv = self.bind.execute(
                select([self.table.c.value], self.table.c.key == key).limit(1)
             ).scalar()

        if not rv:
            raise KeyError(key)

        return pickle.loads(rv)

    def _put(self, key, data):
        con = self.bind.connect()
        data = pickle.dumps(data)
        
        with con.begin():
            # delete the old
            con.execute(self.table.delete(self.table.c.key == key))

            # insert new
            con.execute(self.table.insert({
                'key': key,
                'value': data
            }))

            # commit happens here

        con.close()
        return key


    def put(self, key, data):
        """Store into key from file
        Stores bytestring *data* in *key*.
        :param key: The key under which the data is to be stored
        :param data: Data to be stored into key, must be `bytes`.
        :returns: The key under which data was stored
        :raises exceptions.ValueError: If the key is not valid.
        :raises exceptions.IOError: If storing failed or the file could not
                                    be read
        """
        self._check_valid_key(key)
        #if not isinstance(data, bytes):
        #    raise IOError("Provided data is not of type bytes")
        return self._put(key, data)



