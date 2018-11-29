from beem.blockchain import Blockchain
from beem.account import Account, Accounts
from beem.utils import formatTimeString, addTzInfo
from beem.nodelist import NodeList
from beem import Steem
from datetime import datetime
from beem.instance import set_shared_steem_instance
import time
import math
import pprint
from steemua.block_ops_storage import BlockTrx, AccountTrx, Account2Trx, FollowsTrx
from steemua.storage import LockDB
import dataset
import os
import json
import csv
from pymongo import MongoClient, UpdateOne, InsertOne, DeleteMany, ReplaceOne


def basic_ua1(d, ua_sum, N):
    ua = (1 - d) + d * ua_sum
    return ua

def basic_ua2(d, ua_sum, N):
    ua = ((1 - d) / N) + d * ua_sum
    return ua


def f1(x, N):

    score = math.log10(x * N + 1/N)*2 +1
    if score < 0:
        score = 0
    if score > 10:
        score = 10
    return round(score, 3)

if __name__ == "__main__":
    
    config_file = 'config.json'
    config_file = 'config_pg.json'
    if not os.path.isfile(config_file):
        block_diff_for_db_storage = 1000
        databaseConnector = "sqlite://db.sqlite"
    else:    
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        #print(config_data)
        databaseConnector = config_data["databaseConnector"]
        # databaseConnector2 = config_data["databaseConnector2"]
        if "follow_db_batch_size" in config_data:
            follow_db_batch_size = config_data["follow_db_batch_size"]
        else:
            follow_db_batch_size = 1000
        with open("config_mongo.json") as json_data_file:
            config_data_mongo = json.load(json_data_file)
        databaseConnector_mongo = config_data_mongo["databaseConnector"]      

    use_mongodb = True
    if use_mongodb:
        client = MongoClient(databaseConnector_mongo)
        db_mongo = client['steemua']
        accounts2_col = db_mongo["accounts2"]
        conf_col = db_mongo["configuration"]
    else:
        db = dataset.connect(databaseConnector2)    
        account2TrxStorage = Account2Trx(db, tablename="today")
    
    db = dataset.connect(databaseConnector)
    lockStorage = LockDB(db)
    print("build_ua_lock %s" % str(lockStorage.is_locked("build_ua_lock")))
    if lockStorage.is_locked("build_ua_lock"):
        print("stop ua100")
        exit()    
    print("read_lock %s" % str(lockStorage.is_locked("read_lock")))
    if lockStorage.is_locked("read_lock"):
        print("stop ua100")
        exit()            
    print("read_lock %s" % str(lockStorage.is_locked("read_lock")))
    if lockStorage.is_locked("read_lock"):
        print("stop stream_post")
        exit()      
    cnt = 0
    continue_cnt = 1
  
    db_data = []
    db_account = []
    db_follows = []
    reverse_id = {}
    accounts_list = []
    zeros_trustscore = 0
    latest_created = None
    latest_block_num = 0
    if use_mongodb:
        cnt = 0
        accounts = {}
        for f in accounts2_col.find({}, {"followers": 0, "following": 0, "history": 0, "last_post": 0, "last_root_post": 0, "last_vote_time": 0}):
            f["following"] = f["following_count"]
            f["followers"] = f["followers_count"]
            if "trust_score" not in f:
                f["trust_score"] = 0.0
                zeros_trustscore += 1
            elif f["trust_score"] == 0:
                zeros_trustscore += 1
            if "last_trust_score" not in f:
                f["last_trust_score"] = 0.0
            if "rank" not in f:
                f["rank"] = 0
            if "trust_score_norm" not in f:
                f["trust_score_norm"] = 0.0
            if "trust_score_n" not in f:
                f["trust_score_n"] = 0.0
            if "created_at" not in f:
                f["created_at"] = None
            elif latest_created is None:
                latest_created = f["created_at"]
                latest_block_num = f["block_num"]
            elif isinstance(f["created_at"], str):
                if addTzInfo(latest_created) < formatTimeString(f["created_at"]):
                    latest_created = formatTimeString(f["created_at"]).replace(tzinfo=None)
                f["created_at"] = formatTimeString(f["created_at"]).replace(tzinfo=None)
                print(f["name"])
            if f["created_at"] is not None and latest_created < f["created_at"]:
                latest_created = f["created_at"]
                latest_block_num = f["block_num"]            
            accounts[f["name"]] = f     
            cnt += 1
            if cnt % 100000 == 0:
                print(cnt)
        print("account with zero trustscore %d" % zeros_trustscore)
        print("latest created %s" % str(latest_created))
    else:
        accounts = account2TrxStorage.get_accounts()
    
    zero_followings = []
    for a in accounts:
        account_id = accounts[a]["id"]
        if accounts[a]["following"] == 0:
            zero_followings.append(account_id)    
    for account in accounts:
        account_id = accounts[account]["id"]
        reverse_id[account_id] = account
        acc = accounts[account]
        accounts_list.append(acc)

    N = len(accounts)
        
    sorted_ua = sorted(accounts_list, key=lambda x: x["trust_score"], reverse=True)
    rang = 0
    top_acc = sorted_ua[0]
    
    nodes = NodeList()
    # nodes.update_nodes(weights={"block": 1})
    nodes.update_nodes()
    node_list = nodes.get_nodes()
    stm = Steem(node=node_list)
    
    b = Blockchain(steem_instance=stm)
    #block_num = b.get_estimated_block_num(top_acc["cached_at"])
    block_num = b.get_estimated_block_num(latest_created)
    

    print("Last blocknumber included: %d \n Updated at: %s (UTC)" % (block_num, formatTimeString(top_acc["cached_at"])))
    top_100 = []
    for f in sorted_ua[:100]:
        rang += 1
        acc = Account(f["name"], steem_instance=stm)
        print("{rank: %d, account: '%s', ua: %.3f, followers: %d, following: %d, rep: %.3f}," % (rang, f["name"], f1(f["trust_score"], N), f["followers"], f["following"], acc.rep))
        top_100.append({"rank": rang, "account": f["name"], "id": f["id"], "ua": f1(f["trust_score"], N), "followers": f["followers"], "following": f["following"], "rep": round(acc.rep, 2)})

    if True:
        all_ua = []
        rang = 0
        for f in sorted_ua:
            rang += 1
            all_ua.append({"rank": rang, "account": f["name"], "N": N, "id": f["id"], "created_at": formatTimeString(f["created_at"]), "ua": f1(f["trust_score"], N), "trust_score": f["trust_score"], "followers": f["followers"], "following": f["following"]})
            accounts[f["name"]]["rank"] = rang


    if use_mongodb:
        lockStorage.lock("write_lock")
        print("write history")
        updated_accounts = []
        cnt = 0
        for f in accounts:
            old_history = None
            if accounts[f]["trust_score"] == 0 and accounts[f]["id"] >= N:
                continue
            for h in accounts2_col.find({"_id": accounts[f]["id"]}, {"history": 1}):
                if "history" in h:
                    old_history = h["history"]
            # old_history = accounts2_col.find({"_id": f["id"]}, {"history": 1})
            history = {"date": latest_created, "N": N, "trust_score": accounts[f]["trust_score"], "rank": accounts[f]["rank"], "ua": f1(accounts[f]["trust_score"], N)}
            if old_history is None:
                updated_accounts.append(UpdateOne({"_id": accounts[f]["id"]}, {"$set": {"last_trust_score": accounts[f]["trust_score"], "rank": accounts[f]["rank"], "history": [history]}}))
            else:
                already_stored = False
                for h in old_history:
                    if h["date"] == latest_created:
                        already_stored = True
                if not already_stored:
                    old_history.append(history)
                    updated_accounts.append(UpdateOne({"_id": accounts[f]["id"]}, {"$set": {"last_trust_score": accounts[f]["trust_score"], "rank": accounts[f]["rank"], "history": old_history}}))
            cnt += 1
            if cnt > 0 and cnt % 100000 == 0:
                print(cnt)
                if len(updated_accounts) > 0:
                    accounts2_col.bulk_write(updated_accounts)
                    updated_accounts = []         

        if len(updated_accounts) > 0:
            accounts2_col.bulk_write(updated_accounts)
            updated_accounts = []
        
        #time.sleep(60)
        lockStorage.unlock("build_ua_lock")
        lockStorage.unlock("write_lock")
        lockStorage.unlock("top100_lock")
        
        write_lock = conf_col.find_one({"key": "latest_block_num"})
        conf_col.update({"_id": write_lock['_id']}, {"$set": {"value": latest_block_num}})        
