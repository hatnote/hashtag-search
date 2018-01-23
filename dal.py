# -*- coding: utf-8 -*-
import os
import oursql
import werkzeug.contrib.cache
from common import (EXCLUDED, PAGINATION)


from log import tlog

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
HT_DB_HOST = 'tools.db.svc.eqiad.wmflabs'
HT_DB_NAME = 's52467__new_hashtags'


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

    def execute(self, query, params, cache_name=None, show_tables=False):
        if cache_name:
            results = Cache.get(cache_name)
            if results:
                return results
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor(oursql.DictCursor, show_table=show_tables)
        try:
            cursor.execute(query, params)
        except Exception as e:
            self.connect()  # Reconnecting
            cursor = self.connection.cursor(oursql.DictCursor, show_table=show_tables)
            cursor.execute(query, params)
        results = cursor.fetchall()
        if cache_name:
            Cache.set(cache_name, results, timeout=CACHE_EXPIRATION)
        return results

    def get_hashtags(self,
                     tag=None,
                     lang=None,
                     start=0,
                     end=PAGINATION,
                     startdate=None,
                     enddate=None):
        if not tag:
            return self.get_all_hashtags(lang=lang,
                                         start=start,
                                         end=end,
                                         startdate=startdate,
                                         enddate=enddate)
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
        AND rc.rc_timestamp/1000000 > ?
        AND rc.rc_timestamp/1000000 <= ?
        ORDER BY rc.rc_timestamp DESC
        LIMIT ?, ?'''
        params = (tag, lang, startdate, enddate, start, end)
        with tlog.critical('get_hashtags') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched revisions tagged with {tag}',
                        tag=tag)
            return ret

    def get_all_hashtags(self,
                         lang=None,
                         start=0,
                         end=PAGINATION,
                         startdate=None,
                         enddate=None):
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
        AND rc.rc_timestamp/1000000 > ?
        AND rc.rc_timestamp/1000000 <= ?
        ORDER BY rc.rc_id DESC
        LIMIT ?, ?''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        params = (lang,) + EXCLUDED + (startdate,) + (enddate,) + (start, end)
        with tlog.critical('get_all_hashtags') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched all hashtags starting at {start}',
                        start=start)
            return ret

    def get_top_hashtags(self, limit=10, recent_count=100000, nobots=True):
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
                                      FROM   recentchanges) - ?
               JOIN hashtags AS ht
                 ON ht.ht_id = htrc.ht_id
        WHERE  ht.ht_text REGEXP '[[:alpha:]]{1}[[:alnum:]]+'
        AND    ht.ht_text NOT IN (%s)
        %s
        GROUP  BY ht.ht_text
        ORDER  BY count DESC
        LIMIT  ?;'''
        query = query_tmpl % (excluded_p, bot_condition)
        params = (recent_count,) + EXCLUDED + (limit,)
        # This query is cached because it's loaded for each visit to
        # the index page
        with tlog.critical('get_top_hashtags') as rec:
            ret = self.execute(query, params, cache_name='top-tags-%s-%s' % (nobots, limit))
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

    def get_hashtag_stats(self,
                          tag,
                          lang=None,
                          startdate=None,
                          enddate=None):
        if not tag:
            return self.get_all_hashtag_stats(lang=lang, startdate=startdate, enddate=enddate)
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
        AND rc.rc_timestamp/1000000 > ?
        AND rc.rc_timestamp/1000000 <= ?
        ORDER BY rc.rc_id DESC'''
        params = (tag, lang, startdate, enddate)
        with tlog.critical('get_hashtag_stats') as rec:
            ret = self.execute(query, params)
            rec.success('Fetched stats for {tag}',
                        tag=tag)
            return ret

    def get_all_hashtag_stats(self, lang=None, startdate=None, enddate=None):
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
        AND rc.rc_timestamp/1000000 > ?
        AND rc.rc_timestamp/1000000 <= ?
        AND ht.ht_text NOT IN(%s)
        AND ht.ht_text REGEXP '[[:alpha:]]+' ''' % ', '.join(['?' for i in range(len(EXCLUDED))])
        with tlog.critical('get_all_hashtag_stats') as rec:
            ret = self.execute(query, (lang,) + (startdate,) + (enddate,) + EXCLUDED)
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

    def get_run_log(self, limit=50000):
        query = '''
        SELECT *
        FROM start_log AS sl 
        JOIN complete_log AS cl 
        ON sl.run_uuid = cl.run_uuid 
        WHERE cl.complete_timestamp > DATE_SUB(NOW(), INTERVAL 3 DAY)
        ORDER BY cl.complete_timestamp DESC
        LIMIT ?'''
        with tlog.critical('get_run_log') as rec:
            return self.execute(query, (limit,), show_tables=True)

    def get_lang_run_log(self, lang, limit=50000, days=3):
        query = '''
        SELECT *
        FROM start_log AS sl 
        JOIN complete_log AS cl 
        ON sl.run_uuid = cl.run_uuid 
        WHERE cl.lang = ? 
        AND cl.complete_timestamp > DATE_SUB(NOW(), INTERVAL ? DAY)
        ORDER BY cl.complete_timestamp DESC
        LIMIT ?'''
        with tlog.critical('get_run_log') as rec:
            return self.execute(query, (lang, days, limit), show_tables=True)
