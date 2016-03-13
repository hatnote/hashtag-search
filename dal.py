# -*- coding: utf-8 -*-
import os
import oursql
import werkzeug.contrib.cache
from common import (EXCLUDED, PAGINATION)


from log import tlog

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
HT_DB_HOST = 's1.labsdb'  # The hashtag table is on the same server as the enwiki db replica
HT_DB_NAME = 's52467__hashtags'


CACHE_EXPIRATION = 5 * 60
_cur_dir = os.path.dirname(__file__)
_cache_dir = os.path.join(_cur_dir, '../cache')
Cache = werkzeug.contrib.cache.FileSystemCache(_cache_dir)


class HashtagDatabaseConnection(object):
    def __init__(self):
        self.connect()

    def connect(self, read_default_file=DB_CONFIG_PATH):
        with tlog.critical('connect') as rec:
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
            cursor = self.connection.cursor(oursql.DictCursor)
            cursor.execute(query, params)
        results = cursor.fetchall()
        if cache_name:
            Cache.set(cache_name, results, timeout=CACHE_EXPIRATION)
        return results

    def get_hashtags(self,
                     tag=None,
                     lang=None,
                     start=0,
                     end=PAGINATION):
        if not tag:
            return self.get_all_hashtags(lang=lang, start=start, end=end)
        if tag and tag[0] == '#':
            tag = tag[1:]
        if not lang:
            lang = '%'
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE ht.ht_text = ?
        AND rc.htrc_lang LIKE ?
        ORDER BY rc.rc_timestamp DESC
        LIMIT ?, ?'''
        params = (tag, lang, start, end)
        with tlog.critical('get_hashtags') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched revisions tagged with {tag}',
                        tag=tag)
            return ret

    def get_all_hashtags(self, lang=None, start=0, end=PAGINATION):
        """Rules for hashtags:
        1. Does not include MediaWiki magic words
        (like #REDIRECT) or parser functions
        2. Must be longer than one character
        3. Must contain at least one non-numeric
        character.
        """
        if not lang:
            lang = '%'
        query = '''
        SELECT *
        FROM recentchanges AS rc
        JOIN hashtag_recentchanges AS htrc
        ON htrc.htrc_id = rc.htrc_id
        JOIN hashtags AS ht
        ON ht.ht_id = htrc.ht_id
        WHERE rc.rc_type = 0
        AND rc.htrc_lang LIKE ?
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+'
        AND CHAR_LENGTH(ht.ht_text) > 1
        ORDER BY rc.rc_id DESC
        LIMIT ?, ?''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        params = (lang,) + EXCLUDED + (start, end)
        with tlog.critical('get_all_hashtags') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched all hashtags starting at {start}',
                        start=start)
            return ret

    def get_top_hashtags(self, limit=10, nobots=True):
        """Gets the top hashtags from an arbitrarily "recent" group of edits
        (not all time).
        """
        excluded_p = ', '.join(['?' for i in range(len(EXCLUDED))])
        if nobots:
            bot_condition = 'AND rc_bot = 0'
        else:
            bot_condition = ''
        query_tmpl = '''
        SELECT ht.ht_text,
               COUNT(ht.ht_text) AS count
        FROM   recentchanges AS rc
               JOIN hashtag_recentchanges AS htrc
                 ON htrc.htrc_id = rc.htrc_id
                    AND rc.htrc_id > (SELECT MAX(htrc_id)
                                      FROM   recentchanges) - %s
               JOIN hashtags AS ht
                 ON ht.ht_id = htrc.ht_id
        WHERE  ht.ht_text REGEXP '[[:alpha:]]{1}[[:alnum:]]+'
        AND    ht.ht_text NOT IN (%s)
        %s
        GROUP  BY ht.ht_text
        ORDER  BY count DESC
        LIMIT  ?;'''
        recent_count = 100000
        query = query_tmpl % (recent_count, excluded_p, bot_condition)
        params = EXCLUDED + (limit,)
        # This query is cached because it's loaded for each visit to
        # the index page
        with tlog.critical('get_top_hashtags') as rec:
            ret = self.execute(query, params, cache_name='top-tags-%s' % nobots)
            rec.success('Fetched top tags with limit of {limit}',
                        limit=limit)
            return ret

    def get_langs(self):
        query = '''
        SELECT htrc_lang
        FROM recentchanges
        GROUP BY htrc_lang'''
        params = ()
        with tlog.critical('get_langs') as rec:
            ret = self.execute(query, params, cache_name='langs')
            rec.success('Fetched available languages')
            return ret

    def get_hashtag_stats(self, tag, lang=None):
        if not tag:
            return self.get_all_hashtag_stats(lang=lang)
        if tag and tag[0] == '#':
            tag = tag[1:]
        if not lang:
            lang = '%'
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
        AND rc.htrc_lang LIKE ?
        ORDER BY rc.rc_id DESC'''
        params = (tag, lang)
        with tlog.critical('get_hashtag_stats') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched stats for {tag}',
                        tag=tag)
            return ret

    def get_all_hashtag_stats(self, lang=None):
        # TODO: Add conditions here
        if not lang:
            lang = '%'
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
        AND rc.htrc_lang LIKE ?
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+' ''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        with tlog.critical('get_all_hashtag_stats') as rec:
            ret = self.execute(query, (lang,) + EXCLUDED)
            rec.success('Fetched all hashtag stats')
            return ret

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
