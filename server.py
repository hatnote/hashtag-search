# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta

from clastic import Application, render_basic, Middleware
from clastic.meta import MetaApplication
from clastic.render import AshesRenderFactory
from clastic.static import StaticApplication

from boltons.strutils import find_hashtags
from boltons.tbutils import ExceptionInfo

from dal import (get_hashtags, get_all_hashtags, get_top_hashtags)


FLUP_LOG_DIR = os.path.expanduser('~')
TEMPLATES_PATH = 'templates'
STATIC_PATH = 'static'

_CUR_PATH = os.path.dirname(__file__)


def format_timestamp(timestamp):
    _timestamp_pattern = '%Y%m%d%H%M%S'
    timestamp = datetime.strptime(timestamp, _timestamp_pattern)
    return timestamp.strftime('%d %b %Y %H:%M:%S')


def process_revs(rev):
    url_str = 'https://%s.wikipedia.org/wiki/?diff=%s&oldid=%s'
    rev['spaced_title'] = rev.get('rc_title', '').replace('_', ' ')
    rev['diff_size'] = rev['rc_new_len'] - rev['rc_old_len']
    rev['date'] = format_timestamp(rev['rc_timestamp'])
    rev['diff_url'] = url_str % (rev['htrc_lang'],
                                 rev['rc_this_oldid'],
                                 rev['rc_last_oldid'])
    rev['tags'] = find_hashtags(rev['rc_comment'])
    for tag in rev['tags']:
        # TODO: Turn @mentions into links
        link = '<a href="/hashtags/search/%s">#%s</a>' % (tag, tag)
        new_comment = rev['rc_comment'].replace('#%s' % tag, link)
        rev['rc_comment'] = new_comment
    return rev


def home():
    top_tags = get_top_hashtags()
    return {'top_tags': top_tags}


def generate_report(request, tag=None, offset=0):
    if tag:
        revs = get_hashtags(tag.lower(), offset)
        tag = '#' + tag
    else:
        revs = get_all_hashtags()
        tag = 'All hashtags'
    ret = [process_revs(rev) for rev in revs]
    users = set([r['rc_user_text'] for r in ret])
    return {'revisions': ret, 
            'tag': tag, 
            'total_revs': len(ret),
            'total_users': len(users),
            'total_bytes': '{:,}'.format(sum([abs(r['diff_size']) for r in ret])),
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
              ('/search/<tag>/<offset>', generate_report, 'report.html'),
              ('/static', StaticApplication(_static_dir)),
              ('/meta/', MetaApplication())]
    return Application(routes, 
                       middlewares=[],
                       render_factory=templater)


if __name__ == '__main__':
    app = create_app()
    app.serve()
