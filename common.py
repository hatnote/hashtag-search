# -*- coding: utf-8 -*-

'''
Excluded tags
~~~~~~~~~~~~~
These tags are excluded from the list of all hashtags and popular
tags. They are used frequently in the edit summary, but not as
hashtags. 

Other MediaWiki magic words and parser functions may contain the
hashmark: https://www.mediawiki.org/wiki/Help:Magic_words
'''
EXCLUDED = ('redirect',
            'ifexist',
            'switch', 
            'ifexpr',
            'if')

# Number of results to display per page
PAGINATION = 25
MAX_DB_ROW = 18446744073709551615  # "some large number" for all rows
# See http://dev.mysql.com/doc/refman/5.0/en/select.html#id4651990
