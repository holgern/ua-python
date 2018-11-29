# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes
from builtins import object
from beemgraphenebase.py23 import py23_bytes, bytes_types
from sqlalchemy.dialects.postgresql import insert as pg_insert
import shutil
import time
import os
import json
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


class BlockTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_ops'

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

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def get_count(self):
        table = self.db[self.__tablename__]
        return table.count()

    def add_batch(self, data, chunk_size=1000):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert_many(data, chunk_size=chunk_size)

    def get_latest_block_num(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-block_num')
        if op is None:
            return None
        return op["block_num"]

    def get_latest_timestamp(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-timestamp')
        if op is None:
            return None
        return op["timestamp"]

    def get_block(self, block_num):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(block_num=block_num):
            ret.append(op)
        return ret

    def get_block_range(self, block_num, _limit):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(table.table.columns.block_num >= block_num, _limit=_limit, order_by='block_num'):
            ret.append(op)
        return ret

    def get_all_block(self):
        ret = []
        table = self.db[self.__tablename__]
        for t in table:
            yield t

    def get_block_trx_id(self, block_num):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(block_num=block_num):
            ret.append(op["trx_id"])
        return ret
    
    def get_block_id(self, block_num):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(block_num=block_num):
            ret.append(op["block_id"])
        return ret

    def update_trx_num(self, block_num, trx_id, op_num, trx_num):
        table = self.db[self.__tablename__]
        data = dict(block_num=block_num, trx_id=trx_id, op_num=op_num, trx_num=trx_num)
        table.update(data, ['block_num', 'trx_id', 'op_num'])

    def get_ops(self, op_type=None, limit=None, offset=0, streamed=False):
        table = self.db[self.__tablename__]
        if op_type is None:
            return table.find(_limit=limit, _offset=offset, _streamed=streamed, order_by='block_num')
        return table.find(type=op_type, _limit=limit, _offset=offset, _streamed=streamed, order_by='block_num')

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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


class AccountTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_accounts'

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
 

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE `steemua`.`steemua_accounts` ( `block_num` INT NOT NULL , `type` varchar(50) NOT NULL, `account_index` INT NOT NULL, `account_name` varchar(50) DEFAULT NULL, PRIMARY KEY (`account_index`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def add_batch(self, data, chunk_size=1000):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert_many(data, chunk_size=chunk_size)

    def get_index(self, account_name):
        table = self.db[self.__tablename__] 
        ret = table.find_one(account_name=account_name)
        if ret is None:
            return None
        return ret["account_index"]

    def get_latest_index(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-account_index')
        if ret is None:
            return None
        return ret["account_index"]

    def get_accounts(self):
        table = self.db[self.__tablename__]
        accounts = {}
        for account in table.find(order_by='account_index'):
            accounts[account["account_name"]] = account["account_index"]
        return accounts

    def get(self):
        table = self.db[self.__tablename__]
        accounts = []
        for account in table.find(order_by='account_index'):
            accounts.append(account)
        return accounts

    def update(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.upsert(d, ["account_index"])
        self.db.commit()

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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


class Account2Trx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_accounts2'

    def __init__(self, db, tablename='steemua_accounts2'):
        self.db = db
        self.__tablename__ = tablename

    def exists_table(self):
        """ Check if the database table exists
        """

        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False
 

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE `steemua`.`steemua_accounts2` ( `id` INT NOT NULL , `name` varchar(16) NOT NULL,  `created_at` DATETIME NOT NULL, `block_num` INT NOT NULL,  `reputation` FLOAT(6) NOT NULL,  `ua` FLOAT(12) NOT NULL,  `followers` INT NOT NULL, `following` INT NOT NULL, `active_at` DATETIME NOT NULL, `cached_at` DATETIME NOT NULL, PRIMARY KEY (`id`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["id"])
        self.db.commit()

    def add_batch(self, data, chunk_size=1000):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            table.insert_many(data, chunk_size=chunk_size)
            # for d in data:
            #    table.upsert(d, ["id"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["id"])            
            
            self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["id"])
        else:
            for d in data:
                table.update(data[d], ["id"])            
        self.db.commit()

    def get_index(self, account_name):
        table = self.db[self.__tablename__] 
        ret = table.find_one(account_name=account_name)
        if ret is None:
            return None
        return ret["id"]

    def get_latest_index(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-id')
        if ret is None:
            return None
        return ret["id"]

    def get_latest_block_num(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-id')
        if ret is None:
            return None
        return ret["block_num"]

    def get_latest_timestamp(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-id')
        if ret is None:
            return None
        return ret["created_at"]

    def get_accounts(self):
        table = self.db[self.__tablename__]
        accounts = {}
        for account in table.find(order_by='id'):
            accounts[account["name"]] = account
        return accounts

    def get_accounts_list(self):
        table = self.db[self.__tablename__]
        accounts = []
        for account in table.find(order_by='id'):
            accounts.append(account)
        return accounts

    def get_account_ids(self):
        table = self.db[self.__tablename__]
        accounts = {}
        for account in table.find(order_by='id'):
            accounts[account["name"]] = account["id"]
        return accounts

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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



class FollowsTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_follows'

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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["follower", "following"])    
        self.db.commit()

    def get(self, follower, following):
        table = self.db[self.__tablename__]
        entry = table.find_one(follower=follower, following=following)
        if entry is None:
            return None
        return entry

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.upsert(d, ["follower", "following"])
            
        self.db.commit()

    def add_batch_pg(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            # statement = pg_insert(table.table).values(d).on_conflict_do_update(index_elements=["follower", "following"], index_where=(table.table.c.follower == d["follower"], table.table.c.following == d["following"]), set_=d)
            statement = "insert into %s(follower, following, state, block_num) values(%d, %d, %d, %d) on conflict(follower, following) do update set state = %d, block_num = %d;" % (self.__tablename__, d["follower"], d["following"], 
                                                                                                                                                                      d["state"], d["block_num"], d["state"], d["block_num"])
            self.db.query(statement)
        self.db.commit()    

    def follows(self, follower, following):
        entry = self.get(follower, following)
        if entry is None:
            return False
        return entry["state"] == 1

    def get_following(self, follower):
        table = self.db[self.__tablename__]
        following_list = []
        for following in table.find(follower=follower, state=1):
            following_list.append(following)
        return following_list

    def get_follower(self, following):
        table = self.db[self.__tablename__]
        follower_list = []
        for follower in table.find(following=following, state=1):
            follower_list.append(follower)
        return follower_list   

    def get_following_count(self, follower):
        table = self.db[self.__tablename__]
        return table.count(follower=follower, state=1)

    def get_follower_count(self, following):
        table = self.db[self.__tablename__]
        return table.count(following=following, state=1)

    def count(self):
        table = self.db[self.__tablename__]
        return table.count()

    def get_latest_created_at(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-block_num')
        if op is None:
            return None
        return op["block_num"]

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


class HistoryTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'ua_history'

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
 
    def add_batch(self, data, chunk_size=1000):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert_many(data, chunk_size=chunk_size)

    def get_latest_created_at(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-created_at')
        if op is None:
            return None
        return op["created_at"]

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


class PostsTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_posts'

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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm"])
        self.db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["authorperm"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["authorperm"])            
            
        self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm"])
        else:
            for d in data:
                table.update(data[d], ["authorperm"])            
        self.db.commit()

    def get_latest_post(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-block_num')
        if ret is None:
            return None
        return ret["block_num"]

    def get_latest_update(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-updated')
        if ret is None:
            return None
        return ret["updated"]

    def get_author_posts(self, author):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(author=author, order_by='-ua_score'):
            posts.append(post)
        return posts

    def get_authorperm_posts(self, authorperm):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(authorperm=authorperm, order_by='-ua_score'):
            posts.append(post)
        return posts

    def get_top_posts(self, oldest_created, min_ua_author=2, min_ua_post=30, max_payout=15, limit=20):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(table.table.columns.payout < max_payout, table.table.columns.created > oldest_created, table.table.columns.ua_author > min_ua_author,
                               table.table.columns.ua_post > min_ua_post, _limit=limit, order_by='-ua_score'):
            posts.append(post)
        return posts

    def get_posts(self):
        table = self.db[self.__tablename__]
        posts = {}
        for post in table.find(order_by='created'):
            posts[post["authorperm"]] = post
        return posts

    def get_post(self, authorperm):
        table = self.db[self.__tablename__]
        posts = None
        for post in table.find(authorperm=authorperm):
            posts = post
        return posts

    def get_posts_list(self):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(order_by='created'):
            posts.append(post)
        return posts

    def get_authorperm(self):
        table = self.db[self.__tablename__]
        posts = {}
        for post in table.find(order_by='created'):
            posts[post["authorperm"]] = post["authorperm"]
        return posts

    def update_voted(self, authorperm, voted):
        table = self.db[self.__tablename__]
        data = dict(authorperm=authorperm, voted=voted)
        table.update(data, ['authorperm'])

    def update_utopian(self, authorperm, utopian, ua_utopian):
        table = self.db[self.__tablename__]
        data = dict(authorperm=authorperm, utopian=utopian, ua_utopian=ua_utopian)
        table.update(data, ['authorperm'])

    def get_authorperm_list(self):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(order_by='created'):
            posts.append(post["authorperm"])
        return posts

    def delete_old_posts(self, days):
        table = self.db[self.__tablename__]
        del_posts = []
        for post in table.find(order_by='created'):
            if (datetime.utcnow() - post["created"]).total_seconds() > 60 * 60 * 24 * days:
                del_posts.append(post["authorperm"])
        for post in del_posts:
            table.delete(authorperm=post)

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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


class VotesTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_votes'

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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm", "voter"])
        self.db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["authorperm", "voter"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["authorperm", "voter"])            
            
        self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm", "voter"])
        else:
            for d in data:
                table.update(data[d], ["authorperm", "voter"])            
        self.db.commit()

    def get_latest_vote(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-time')
        if ret is None:
            return None
        return ret["time"]

    def get_votes(self, authorperm):
        table = self.db[self.__tablename__]
        votes = {}
        for v in table.find(authorperm=authorperm):
            votes[v["voter"]] = v
        return votes

    def delete_old_votes(self, days):
        table = self.db[self.__tablename__]
        del_posts = []
        for post in table.find(order_by='time'):
            if (datetime.utcnow() - post["time"]).total_seconds() > 60 * 60 * 24 * days:
                del_posts.append({"authorperm": post["authorperm"], "voter": post["voter"]})
        for post in del_posts:
            table.delete(authorperm=post["authorperm"], voter=post["voter"])

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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

class AccountVotesTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_account_votes'

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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm"])
        self.db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["authorperm"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["authorperm"])            
            
        self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm"])
        else:
            for d in data:
                table.update(data[d], ["authorperm"])            
        self.db.commit()

    def get_latest_vote(self, author):
        table = self.db[self.__tablename__]
        ret = table.find_one(author=author, order_by='-timestamp')
        if ret is None:
            return None
        return ret["timestamp"]

    def get_votes(self, author):
        table = self.db[self.__tablename__]
        votes = []
        for v in table.find(author=author, order_by='-timestamp'):
            votes.append(v)
        return votes

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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


class MemberTrx(object):
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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["name"])
        self.db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["name"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["name"])            
            
        self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["name"])
        else:
            for d in data:
                table.update(data[d], ["name"])            
        self.db.commit()

    def get_latest_delegation(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by='-delegation_timestamp')
        if ret is None:
            return None
        return ret["delegation_timestamp"]

    def get_all_member(self):
        table = self.db[self.__tablename__]
        member = {}
        for v in table.all():
            member[v["name"]] = v
        return member


    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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


class UaTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steemua_ua'

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
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["name"])
        self.db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["name"])
        else:
            self.db.begin()
            for d in data:
                table.upsert(data[d], ["name"])            
            
        self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["name"])
        else:
            for d in data:
                table.update(data[d], ["name"])            
        self.db.commit()

    def get_all_accounts(self):
        table = self.db[self.__tablename__]
        member = {}
        for v in table.all():
            member[v["name"]] = v
        return member


    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

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

