# -*- coding: UTF-8 -*-
from flup.server.fcgi import WSGIServer
from clastic import Application, render_json, render_basic
from clastic.meta import MetaApplication
from clastic.render import AshesRenderFactory
import oursql
import os

DB_CONFIG_PATH = os.path.expanduser('~/replica.my.cnf')
TEMPLATES_PATH = 'templates'
DEFAULT_LANG = 'en'
DEFAULT_DAYS = 3


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
    cursor.execute(query, (days, '(^| )[#]{1}' + tag + '[[:>:]]'))
    ret = cursor.fetchall()
    return ret


def process_revs(rev, lang):
    url_str = '%s.wikipedia.org/wiki/?diff=%s&oldid=%s'
    rev['diff_size'] = rev['rc_new_len'] - rev['rc_old_len']
    rev['date'] = rev['rc_timestamp']  # TODO
    rev['diff_url'] = url_str % (rev['lang'],
                                 rev['rc_this_oldid'],
                                 rev['rc_last_oldid'])
    return rev


def generate_report(tag, lang=DEFAULT_LANG, days=DEFAULT_DAYS):
    revs = get_hashtags(tag, lang, days)
    ret = [process_revs(rev, lang) for rev in revs]
    return {'revisions': ret, 'tag': tag}


def create_app():
    templater = AshesRenderFactory(TEMPLATES_PATH)
    routes = [('/<tag>', generate_report, 'design.html'),
              ('/get/<tag>', get_hashtags, render_json),
              ('/get/<tag>/<lang>', get_hashtags, render_json),
              ('/get/<tag>/<lang>/<days>', get_hashtags, render_json),
              ('/meta', MetaApplication()),
              ('/_dump_environ', lambda request: request.environ, templater)]
    return Application(routes)


if __name__ == '__main__':
    wsgi_app = create_app()
    wsgi_server = WSGIServer(wsgi_app)
    wsgi_server.run()
