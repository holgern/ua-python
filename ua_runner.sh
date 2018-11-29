#!/bin/bash
beempy updatenodes

/usr/bin/python3.6 -u /root/git/ua-python/streamCustomJsonOps.py
/usr/bin/python3.6 -u /root/git/ua-python/build_follow_db.py
/usr/bin/python3.6 -u /root/git/ua-python/init_accounts_for_ua.py
/usr/bin/python3.6 -u /root/git/ua-python/set_trusted_accounts.py

/usr/bin/python3.6 -u /root/git/ua-python/build_ua3.py

/usr/bin/python3.6 -u /root/git/ua-python/ua_top100.py



