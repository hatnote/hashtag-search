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
DEFAULT_DAYS = 3

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


def get_hashtags(tag=None, start_date=None, end_date=None, lang=DEFAULT_LANG):
    _date_pattern = '%Y%m%d%H%M%S'
    query = '''
        SELECT *
        FROM recentchanges
        WHERE rc_type = 0
        AND rc_timestamp BETWEEN ? AND ?
        AND rc_comment REGEXP ? ORDER BY rc_timestamp DESC'''
    db_title = lang + 'wiki_p'
    db_host = lang + 'wiki.labsdb'
    connection = oursql.connect(db=db_title,
                                host=db_host,
                                read_default_file=DB_CONFIG_PATH,
                                charset=None,
                                use_unicode=True)
    cursor = connection.cursor(oursql.DictCursor)
    if tag is None:
        tag_pattern = '(^| )#[[:alpha:]]{1}[[:alnum:]]*[[:>:]]'
    else:
        tag_pattern = '(^| )#%s[[:>:]]' % tag
    start_date, end_date = default_dates(start_date, end_date)
    start_date = start_date.strftime(_date_pattern)
    end_date = end_date.strftime(_date_pattern)
    cursor.execute(query, (start_date, end_date, tag_pattern))
    ret = cursor.fetchall()
    return ret


def process_revs(rev, lang):
    url_str = 'https://%s.wikipedia.org/wiki/?diff=%s&oldid=%s'
    rev['spaced_title'] = rev.get('rc_title', '').replace('_', ' ')
    rev['diff_size'] = rev['rc_new_len'] - rev['rc_old_len']
    rev['date'] = format_timestamp(rev['rc_timestamp'])
    rev['diff_url'] = url_str % (lang,
                                 rev['rc_this_oldid'],
                                 rev['rc_last_oldid'])
    rev['tags'] = find_hashtags(rev['rc_comment'])
    return rev


def home():
    pass


def generate_report(request, tag=None, lang=DEFAULT_LANG, days=DEFAULT_DAYS):
    _date_pattern  = '%Y-%m-%d'
    start_date = request.values.get('start-date')
    end_date = request.values.get('end-date')
    if end_date:
        end_date = datetime.strptime(end_date, _date_pattern)
    else:
        end_date = datetime.now()
    if start_date:
        start_date = datetime.strptime(start_date, _date_pattern)
    else:
        start_date = end_date - timedelta(days=DEFAULT_DAYS)
    lang = request.values.get('lang', DEFAULT_LANG).lower()
    revs = get_hashtags(tag, start_date, end_date, lang)
    ret = [process_revs(rev, lang) for rev in revs]
    ret = [r for r in ret if not all(tag.lower() == 'redirect' for tag
                                     in r['tags'])]
    if tag is None:
        tag = 'all tags'
    return {'revisions': ret, 
            'tag': tag, 
            'start_date': start_date.strftime('%Y-%m-%d'),  # TODO
            'end_date': end_date.strftime('%Y-%m-%d'),  # TODO
            'lang': lang
    }


def create_app():
    _template_dir = os.path.join(_CUR_PATH, TEMPLATES_PATH)
    _static_dir = os.path.join(_CUR_PATH, STATIC_PATH)
    templater = AshesRenderFactory(_template_dir)
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
