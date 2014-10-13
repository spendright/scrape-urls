import logging
from argparse import ArgumentParser
from traceback import print_exc
from urllib2 import HTTPError

from srs.db import download_db
from srs.db import open_db
from srs.db import open_dt
from srs.db import show_tables
from srs.scrape import scrape_facebook_url
from srs.scrape import scrape_soup
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


def main():
    opts = parse_args()

    log_to_stderr(verbose=opts.verbose, quiet=opts.quiet)

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

    dt = open_dt()
    failed_urls = []

    for i, url in enumerate(sorted(all_urls)):
        log.info('scraping {} ({} of {})'.format(
            url, i + 1, len(all_urls)))

        try:
            row = dict(url=url, http_status=200)
            soup = None
            try:
                soup = scrape_soup(url)
            # we want to know if URL no longer works
            except HTTPError as e:
                row.http_status = e.code

            if soup:
                row['twitter_handle'] = scrape_twitter_handle(
                    soup, required=False)
                row['facebook_url'] = scrape_facebook_url(
                    soup, required=False)

            dt.upsert(row, 'url')
        except:
            failed_urls.append(url)
            print_exc()

    if failed_urls:
        raise Exception(
            'failed to scrape {} URLs:\n{}'.format(
                len(failed_urls), '\n'.join(failed_urls)))


def parse_args(args=None):
    parser = ArgumentParser()
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
