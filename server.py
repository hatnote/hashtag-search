# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime, timedelta
from os.path import join as pjoin

from clastic import Application, render_json, render_basic, Middleware
from clastic.meta import MetaApplication
from clastic.render import AshesRenderFactory
from clastic.static import StaticApplication
import oursql

from boltons.strutils import find_hashtags
from boltons.tbutils import ExceptionInfo

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
FLUP_LOG_DIR = os.path.expanduser('~')
TEMPLATES_PATH = 'templates'
STATIC_PATH = 'static'
DEFAULT_LANG = 'en'
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50

HT_DB_HOST = 's1.labsdb'  # The hashtag table is on the same server as the enwiki db replica
HT_DB_NAME = 's52490__hashtags_p'

_CUR_PATH = os.path.dirname(__file__)


class ExceptionPrinter(Middleware):
    def request(self, next, request, _route):
        try:
            return next()
        except:
            formatted_tb = ExceptionInfo.from_current().get_formatted()
            print formatted_tb
            import pdb;pdb.post_mortem()
            return render_basic(request=request, 
                                context=formatted_tb, 
                                _route=_route)


def format_timestamp(timestamp):
    _timestamp_pattern = '%Y%m%d%H%M%S'
    timestamp = datetime.strptime(timestamp, _timestamp_pattern)
    return timestamp.strftime('%d %b %Y %H:%M:%S')


def default_dates(start, end):
    if not end:
        end = datetime.now()
        print end
    if not start:
        start = end - timedelta(days=DEFAULT_DAYS)
    return start, end



def ht_db_connect(read_default_file=DB_CONFIG_PATH):
    connection = oursql.connect(db=HT_DB_NAME,
                                host=HT_DB_HOST,
                                read_default_file=read_default_file,
                                charset=None,
                                use_unicode=False)
    return connection


def get_hashtags(tag, start_date=None, end_date=None, lang=DEFAULT_LANG):
    _date_pattern = '%Y%m%d%H%M%S'
    start_date, end_date = default_dates(start_date, end_date)
    start_date = start_date.strftime(_date_pattern)
    end_date = end_date.strftime(_date_pattern)
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
    AND rc.htrc_lang = ?
    AND rc.rc_timestamp BETWEEN ? AND ?
    ORDER BY rc.rc_id DESC'''
    # TODO: Pagination if the results are too big?
    params = (tag, lang, start_date, end_date)
    cursor.execute(query, params)
    return cursor.fetchall()


def get_all_hashtags(lang=DEFAULT_LANG, limit=DEFAULT_LIMIT):
    connection = ht_db_connect()
    cursor = connection.cursor(oursql.DictCursor)
    query = '''
    SELECT *
    FROM recentchanges AS rc
    WHERE rc.rc_type = 0
    ORDER BY rc.rc_id DESC
    LIMIT ?'''
    # TODO: Pagination for the next bunch of revisions
    params = (limit,)
    cursor.execute(query, params)
    return cursor.fetchall()



def process_revs(rev, lang):
    url_str = 'https://%s.wikipedia.org/wiki/?diff=%s&oldid=%s'
    rev['spaced_title'] = rev.get('rc_title', '').replace('_', ' ')
    rev['diff_size'] = rev['rc_new_len'] - rev['rc_old_len']
    rev['date'] = format_timestamp(rev['rc_timestamp'])
    rev['diff_url'] = url_str % (lang,
                                 rev['rc_this_oldid'],
                                 rev['rc_last_oldid'])
    rev['tags'] = find_hashtags(rev['rc_comment'])
    for tag in rev['tags']:
        # TODO: Should the tags column also be hyperlinks?
        # TODO: Turn @mentions into links
        link = '<a href="/hashtags/search/%s">#%s</a>' % (tag, tag)
        new_comment = rev['rc_comment'].replace('#%s' % tag, link)
        rev['rc_comment'] = new_comment
    return rev


def home():
    pass


def generate_report(request, tag=None, lang=DEFAULT_LANG, days=DEFAULT_DAYS):
    _date_pattern  = '%Y-%m-%d'
    start_date = request.values.get('start-date')
    end_date = request.values.get('end-date')
    # TODO: Organize dates
    if end_date:
        end_date = datetime.strptime(end_date, _date_pattern)
    else:
        end_date = datetime.now()
    if start_date:
        start_date = datetime.strptime(start_date, _date_pattern)
    else:
        start_date = end_date - timedelta(days=DEFAULT_DAYS)
    lang = request.values.get('lang', DEFAULT_LANG).lower()
    if tag:
        revs = get_hashtags(tag, start_date, end_date, lang)
    else:
        # TODO: When you get all hashtags, the results tempalte should
        # explain the results.
        revs = get_all_hashtags(lang=lang)
    ret = [process_revs(rev, lang) for rev in revs]
    ret = [r for r in ret if not all(tag.lower() == 'redirect' for tag
                                     in r['tags'])]  
    # TODO: Filter for phrases that are not valid hashtags (like #1)
    # or are mediawiki magic words (like redirect)
    return {'revisions': ret, 
            'tag': tag, 
            'start_date': start_date.strftime('%Y-%m-%d'),  # TODO: Better date handling
            'end_date': end_date.strftime('%Y-%m-%d'),
            'lang': lang}


def create_app():
    _template_dir = os.path.join(_CUR_PATH, TEMPLATES_PATH)
    _static_dir = os.path.join(_CUR_PATH, STATIC_PATH)
    templater = AshesRenderFactory(_template_dir)
    # TODO: Add support for @mentions
    # TODO: Add a list of the most popular tags/mentions on the front page
    routes = [('/', home, 'index.html'),
              ('/search/', generate_report, 'report.html'),
              ('/search/all', generate_report, 'report.html'),
              ('/search/<tag>', generate_report, 'report.html'),
              ('/static', StaticApplication(_static_dir)),
              ('/meta/', MetaApplication())]
    return Application(routes, 
                       middlewares=[],
                       render_factory=templater)


if __name__ == '__main__':
    app = create_app()
    app.serve()
