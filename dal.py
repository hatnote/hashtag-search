# -*- coding: utf-8 -*-
import os
import oursql
import werkzeug.contrib.cache
from common import (EXCLUDED, PAGINATION)


DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
HT_DB_HOST = 's1.labsdb'  # The hashtag table is on the same server as the enwiki db replica
HT_DB_NAME = 's52490__hashtags_p'


import logging
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

CACHE_EXPIRATION = 5 * 60
_cur_dir = os.path.dirname(__file__)
_cache_dir = os.path.join(_cur_dir, '../cache')
Cache = werkzeug.contrib.cache.FileSystemCache(_cache_dir)


class HashtagDatabaseConnection(object):
    def __init__(self):
        self.connect()

    def connect(self, read_default_file=DB_CONFIG_PATH):
        self.connection = oursql.connect(db=HT_DB_NAME,
                                         host=HT_DB_HOST,
                                         read_default_file=read_default_file,
                                         charset=None,
                                         use_unicode=False,
                                         autoping=True)

    def execute(self, query, params, cache_name=None):
        if cache_name:
            results = Cache.get(cache_name)
            if results:
                return results
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor(oursql.DictCursor)
        try:
            cursor.execute(query, params)
        except Exception as e:
            self.connect()  # Reconnecting
            logging.log(logging.DEBUG, e)
            cursor = self.connection.cursor(oursql.DictCursor)
            cursor.execute(query, params)
        results = cursor.fetchall()
        if cache_name:
            Cache.set(cache_name, results, timeout=CACHE_EXPIRATION)
        return results
        
    def get_hashtags(self, tag=None, start=0, end=PAGINATION):
        if not tag:
            return self.get_all_hashtags(start, end)
        if tag and tag[0] == '#':
            tag = tag[1:]
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE ht.ht_text = ?
        ORDER BY rc.rc_timestamp DESC
        LIMIT ?, ?'''
        params = (tag, start, end)
        return self.execute(query, params)

    def get_all_hashtags(self, start=0, end=PAGINATION):
        """Rules for hashtags: 
        1. Does not include MediaWiki magic words
        (like #REDIRECT) or parser functions
        2. Must be longer than one character
        3. Must contain at least one non-numeric
        character.
        """
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE rc.rc_type = 0
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+'
        AND CHAR_LENGTH(ht.ht_text) > 1
        ORDER BY rc.rc_id DESC
        LIMIT ?, ?''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        params = EXCLUDED + (start, end)
        return self.execute(query, params)

    def get_top_hashtags(self, limit=10):
        query = '''
        SELECT ht.ht_text, COUNT(*) as count
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE ht.ht_text REGEXP '[[:alpha:]]{1}[[:alnum:]]*'
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+'
        AND CHAR_LENGTH(ht.ht_text) > 1
        GROUP BY ht.ht_text
        ORDER BY COUNT(*) DESC
        LIMIT ?'''  % ', '.join(['?' for i in range(len(EXCLUDED))])
        params = EXCLUDED + (limit,)
        # This query is cached because it's loaded for each visit to
        # the index page
        return self.execute(query, params, cache_name='top-tags')

    def get_hashtag_stats(self, tag):
        if not tag:
            return self.get_all_hashtag_stats()
        if tag and tag[0] == '#':
            tag = tag[1:]
        query = '''
        SELECT COUNT(*) as revisions,
        COUNT(DISTINCT rc_user) as users,
        COUNT(DISTINCT rc_title) as pages,
        COUNT(DISTINCT htrc_lang) as langs,
        MIN(rc_timestamp) as oldest,
        MAX(rc_timestamp) as newest,
        SUM(ABS(rc_new_len - rc_old_len)) as bytes
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE ht.ht_text = ?
        ORDER BY rc.rc_id DESC'''
        params = (tag,)
        return self.execute(query, params)

    def get_all_hashtag_stats(self):
        # TODO: Add conditions here
        query = '''
        SELECT COUNT(*) as revisions,
        COUNT(DISTINCT rc_user) as users,
        COUNT(DISTINCT rc_title) as pages,
        MIN(rc_timestamp) as oldest,
        MAX(rc_timestamp) as newest,
        SUM(ABS(rc_new_len - rc_old_len)) as bytes
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE rc.rc_type = 0
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+'
        AND CHAR_LENGTH(ht.ht_text) > 1
        ORDER BY rc.rc_id DESC''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        return self.execute(query, EXCLUDED)

    def get_mentions(self, name=None, start=0, end=PAGINATION):
        if not name:
            return self.get_all_mentions(start, end)
        if name and name[0] == '@':
            tag = tag[1:]
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN mention_recentchanges AS mnrc
        ON mnrc.mnrc_id = rc.htrc_id
        JOIN mentions AS mn
        ON mn.mn_id = mnrc.mn_id
        WHERE mn.mn_text = ?
        ORDER BY rc.rc_id DESC
        LIMIT ?, ?'''
        params = (name, start, end)
        return self.execute(query, params)

    def get_all_mentions(self, start=0, end=PAGINATION):
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN mention_recentchanges AS mnrc
        ON mnrc.mnrc_id = rc.htrc_id
        JOIN mentions AS mn
        ON mn.mn_id = mnrc.mn_id
        ORDER BY rc.rc_id DESC
        LIMIT ?, ?'''
        return self.execute(query, (start, end))
