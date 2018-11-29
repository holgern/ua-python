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
