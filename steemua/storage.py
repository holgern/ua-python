# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes
from builtins import object
from beemgraphenebase.py23 import py23_bytes, bytes_types
import shutil
import time
import os
import sqlite3
from appdirs import user_data_dir
from datetime import datetime, timedelta
from beem.utils import formatTimeString, addTzInfo
import logging
from binascii import hexlify
import random
import hashlib
import dataset
from sqlalchemy import and_
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class TrxDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_trx'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False


    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()

    def get_all_op_index(self, source):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(source=source):
            id_list.append(trx["index"])
        return id_list

    def get_account(self, account, share_type="standard"):
        """ Returns all entries for given value
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(account=account, share_type=share_type):
            id_list.append(trx)
        return id_list        

    def get(self, index, source):
        """ Returns all entries for given value
        """
        table = self.db[self.__tablename__]
        return table.find_one(index=index, source=source)

    def get_share_type(self, share_type):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        return table.find(share_type=share_type)

    def get_lastest_share_type(self, share_type):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='-index', share_type=share_type)    

    def get_SBD_transfer(self, account, shares, timestamp):
        """ Returns all entries for given value
        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(account=account, shares=-shares, share_type="SBD"):
            if addTzInfo(trx["timestamp"]) < addTzInfo(timestamp):
                found_trx = trx
        return found_trx

    def update_delegation_shares(self, source, account, shares):
        """ Change share_age depending on timestamp
        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, status="Valid", share_type="Delegation"):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, shares=shares)
        table.update(data, ['index', 'source'])

    def update_delegation_state(self, source, account, share_type_old, share_type_new):
        """ Change share_age depending on timestamp
        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, share_type=share_type_old):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, share_type=share_type_new)
        table.update(data, ['index', 'source'])

    def update_memo(self, source, account, memo_old, memo_new):
        """ Change share_age depending on timestamp
        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, memo=memo_old):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, memo=memo_new)
        table.update(data, ['index', 'source'])

    def update_sponsee(self, source, account, memo, sponsee, status):
        """ Change share_age depending on timestamp
        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, memo=memo):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, sponsee=sponsee, status=status)
        table.update(data, ['index', 'source'])

    def add(self, data):
        """ Add a new data set
        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def delete(self, index, source):
        """ Delete a data set
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(index=index, source=source)

    def delete_all(self, source):
        """ Delete a data set
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(source=source)

    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop


class MemberDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_member'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()
    
    def get_all_accounts(self):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table:
            id_list.append(trx["account"])
        return id_list
    
    def add(self, data):
        """ Add a new data set
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["account"])
        self.db.commit()


    def add_batch(self, data):
        """ Add a new data set
        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.upsert(d, ["account"])
     
        self.db.commit()
    
    def get(self, account):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        return table.find_one(account=account)

    def get_last_updated_member(self):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='-update_at')    
        

    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ['account'])
    
    def delete(self, account):
        """ Delete a data set
    
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(account=account)
    
    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop



class ConfigurationDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_configuration'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def get(self):
        """ Returns the public keys stored in the database
        """
        table = self.db[self.__tablename__]
        return table.find_one(id=1)
    
    def set(self, data):
        """ Add a new data set
    
        """
        data["id"]= 1
        table = self.db[self.__tablename__]
        table.upsert(data, ["id"])
        self.db.commit()


    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        data["id"]= 1
        table = self.db[self.__tablename__]
        table.update(data, ['id'])
    
    def delete(self, account):
        """ Delete a data set
    
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(account=account)
    
    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop


class LockDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_lock'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def get(self, key):
        """ Returns the public keys stored in the database
        """
        table = self.db[self.__tablename__]
        return table.find_one(key=key)
    
    def is_locked(self, key):
        table = self.db[self.__tablename__]
        ret = table.find_one(key=key)
        if 'value' in ret:
            return ret['value']
        else:
            return False

    def lock(self, key):
        """ Add a new data set
    
        """
        data = {"key": key, "value": True}
        table = self.db[self.__tablename__]
        table.upsert(data, ["key"])
        self.db.commit()

    def unlock(self, key):
        """ Add a new data set
    
        """
        data = {"key": key, "value": False}
        table = self.db[self.__tablename__]
        table.upsert(data, ["key"])
        self.db.commit()

    def upsert(self, key, data):
        """ Add a new data set
    
        """
        data["key"] = key
        table = self.db[self.__tablename__]
        table.upsert(data, ["key"])
        self.db.commit()

    def update(self, data):
        """ Change share_age depending on key
    
        """
        table = self.db[self.__tablename__]
        table.update(data, ['key'])
    
    def delete(self, key):
        """ Delete a data set
    
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(key=key)
    
    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop
