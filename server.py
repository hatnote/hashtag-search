# -*- coding: utf-8 -*-
import os
import sys
from os.path import join as pjoin

from flup.server.fcgi import WSGIServer
from clastic import Application, render_json, render_basic
from clastic.meta import MetaApplication
from clastic.render import AshesRenderFactory
from clastic.static import StaticApplication
import oursql


DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
FLUP_LOG_DIR = os.path.expanduser('~')
TEMPLATES_PATH = 'templates'
STATIC_PATH = 'static'
DEFAULT_LANG = 'en'
DEFAULT_DAYS = 3

_CUR_PATH = os.path.dirname(__file__)


def get_hashtags(tag, lang=DEFAULT_LANG, days=DEFAULT_DAYS):
    query = '''
        SELECT *
        FROM recentchanges
        WHERE rc_type = 0
        AND rc_timestamp > DATE_FORMAT(DATE_SUB(NOW(),
                                       INTERVAL ? DAY),
                                       '%Y%m%d%H%i%s')
        AND rc_comment REGEXP ?'''
    db_title = lang + 'wiki_p'
    db_host = lang + 'wiki.labsdb'
    connection = oursql.connect(db=db_title,
                                host=db_host,
                                read_default_file=DB_CONFIG_PATH,
                                charset=None)
    cursor = connection.cursor(oursql.DictCursor)
    if tag is None:
        tag_pattern = '(^| )#[[:alpha:]]{1}[[:alnum:]]*[[:>:]]'
    else:
        tag_pattern = '(^| )[#]{1}%s[[:>:]]' % tag
    cursor.execute(query, (days, tag_pattern))
    ret = cursor.fetchall()
    return ret


def process_revs(rev, lang):
    url_str = '%s.wikipedia.org/wiki/?diff=%s&oldid=%s'
    rev['diff_size'] = rev['rc_new_len'] - rev['rc_old_len']
    rev['date'] = rev['rc_timestamp']  # TODO
    rev['diff_url'] = url_str % (lang,
                                 rev['rc_this_oldid'],
                                 rev['rc_last_oldid'])
    return rev


def generate_report(tag=None, lang=DEFAULT_LANG, days=DEFAULT_DAYS):
    revs = get_hashtags(tag, lang, days)
    ret = [process_revs(rev, lang) for rev in revs]
    if tag is None:
        tag = 'all tags'
    return {'revisions': ret, 'tag': tag}


def create_app():
    _template_dir = os.path.join(_CUR_PATH, TEMPLATES_PATH)
    _static_dir = os.path.join(_CUR_PATH, STATIC_PATH)
    templater = AshesRenderFactory(_template_dir)
    routes = [('/', generate_report, 'design.html'),
              ('/<tag>', generate_report, 'design.html'),
              ('/get/<tag>', get_hashtags, render_json),
              ('/get/<tag>/<lang>', get_hashtags, render_json),
              ('/get/<tag>/<lang>/<days>', get_hashtags, render_json),
              ('/static', StaticApplication(_static_dir)),
              ('/meta', MetaApplication())]
    return Application(routes, render_factory=templater)


if __name__ == '__main__':
    wsgi_app = create_app()
    # we have to redirect stdout and stderr, otherwise we see no
    # errors beyond this point.
    sys.stdout = open(pjoin(FLUP_LOG_DIR, 'flup-stdout.log'), 'a')
    sys.stderr = open(pjoin(FLUP_LOG_DIR, 'flup-stderr.log'), 'a')
    wsgi_server = WSGIServer(wsgi_app)
    wsgi_server.run()
