# -*- coding: utf-8 -*-

import os
import oursql

EXCLUDED = ('redirect', 'ifexist', 'switch', 'ifexpr')
PAGINATION = 25

DEFAULT_LANG = 'en'
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
HT_DB_HOST = 's1.labsdb'  # The hashtag table is on the same server as the enwiki db replica
HT_DB_NAME = 's52490__hashtags_p'


def ht_db_connect(read_default_file=DB_CONFIG_PATH):
    connection = oursql.connect(db=HT_DB_NAME,
                                host=HT_DB_HOST,
                                read_default_file=read_default_file,
                                charset=None,
                                use_unicode=False)
    return connection


def get_hashtags(tag, offset=0, pagination=PAGINATION):
    if tag and tag[0] == '#':
        tag = tag[1:]
    connection = ht_db_connect()
    cursor = connection.cursor(oursql.DictCursor)
    query = '''
    SELECT *
    FROM recentchanges AS rc
    JOIN hashtag_recentchanges AS htrc
      ON htrc.htrc_id = rc.htrc_id
    JOIN hashtags AS ht
      ON ht.ht_id = htrc.ht_id
    WHERE ht.ht_text = ?
    ORDER BY rc.rc_id DESC
    LIMIT ?, ?'''
    params = (tag, offset, pagination)
    cursor.execute(query, params)
    return cursor.fetchall()


def get_hashtag_stats(tag):
    if tag and tag[0] == '#':
        tag = tag[1:]
    connection = ht_db_connect()
    cursor = connection.cursor(oursql.DictCursor)
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
    WHERE ht.ht_text = ?
    ORDER BY rc.rc_id DESC'''
    params = (tag,)
    cursor.execute(query, params)
    return cursor.fetchall()

def get_all_hashtags(offset=0, pagination=PAGINATION):
    connection = ht_db_connect()
    cursor = connection.cursor(oursql.DictCursor)
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
    params = EXCLUDED + (offset, pagination)
    cursor.execute(query, params)
    return cursor.fetchall()


def get_top_hashtags(limit=10):
    connection = ht_db_connect()
    cursor = connection.cursor(oursql.DictCursor)
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
    cursor.execute(query, params)
    return cursor.fetchall()


