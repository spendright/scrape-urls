# -*- coding: utf-8 -*-

#   Copyright 2014 SpendRight, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import logging
from argparse import ArgumentParser
from os import environ
from traceback import print_exc

from bs4 import BeautifulSoup

from srs.db import create_table_if_not_exists
from srs.db import download_db
from srs.db import open_db
from srs.db import open_dt
from srs.db import show_tables
from srs.iso_8601 import iso_now
from srs.scrape import scrape_facebook_url
from srs.scrape import scrape
from srs.scrape import scrape_twitter_handle

from srs.log import log_to_stderr


log = logging.getLogger('scraper')

# DBs to get URLs from
SOURCE_DBS = {
    'campaigns',
    'companies',
}

# url fields we care about
URL_COLUMNS = {
    'author_url',
    'url',
}

# rating tables have urls too, but they don't have twitter handles, etc.
SKIP_TABLES = {
    'campaign_brand_rating',
    'campaign_company_rating',
}

# raise an exception if more than this many URLs fail
MAX_PROPORTION_FAILURES = 0.1


def main():
    opts = parse_args()

    log_to_stderr(verbose=opts.verbose, quiet=opts.quiet)

    if opts.urls:
        all_urls = opts.urls
    elif environ.get('MORPH_URLS'):
        all_urls = filter(None, environ['MORPH_URLS'].split())
    else:
        all_urls = set()

        for db_name in SOURCE_DBS:
            download_db(db_name)
            db = open_db(db_name)
            for table in show_tables(db):
                if table in SKIP_TABLES:
                    continue
                urls = select_urls(db, table)
                if urls:
                    log.info('read {} urls from {}.{}'.format(
                        len(urls), db_name, table))
                all_urls.update(urls)

    create_table_if_not_exists('url', with_scraper_id=False)

    dt = open_dt()
    failures = []  # tuple of (url, exception)

    for i, url in enumerate(sorted(all_urls)):
        log.info('scraping {} ({} of {})'.format(
            url, i + 1, len(all_urls)))

        try:
            html = scrape(url)

            soup = BeautifulSoup(html)
            row = dict(url=url, last_scraped=iso_now())
            row['twitter_handle'] = scrape_twitter_handle(
                soup, required=False)
            row['facebook_url'] = scrape_facebook_url(
                soup, required=False)

            log.debug('`url`: {}'.format(repr(row)))
            dt.upsert(row, 'url')
        except Exception as e:
            failures.append((url, e))
            print_exc()

    # show a summary of failures
    if failures:
        log.warn('Failed to scrape {} of {} URL{}:'.format(
            len(failures), url,
            's' if len(failures) > 2 else ''))
        for url, e in failures:
            log.warn(u'  {}: {}'.format(url, repr(e)))

    if len(failures) > len(all_urls) * MAX_PROPORTION_FAILURES:
        raise Exception('too many failures')


def parse_args(args=None):
    parser = ArgumentParser()
    parser.add_argument('urls', metavar='N', nargs='*',
                        help='urls to scrape')
    parser.add_argument(
        '-v', '--verbose', dest='verbose', default=False, action='store_true',
        help='Enable debug logging')
    parser.add_argument(
        '-q', '--quiet', dest='quiet', default=False, action='store_true',
        help='Turn off info logging')

    return parser.parse_args(args)


def select_urls(db, table):
    """Yield (non-blank) URLs from the given table."""
    urls = set()

    sql = 'select * from `{}`'.format(table)
    for row in db.execute(sql):
        row = dict(row)
        for column in URL_COLUMNS:
            url = row.get(column)
            if url:
                urls.add(url)

    return urls


if __name__ == '__main__':
    main()
