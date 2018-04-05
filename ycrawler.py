import os
import sys
import asyncio
import logging
import traceback
from datetime import datetime
from functools import partial
from optparse import OptionParser
from urllib.parse import urljoin, urldefrag
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import async_timeout


TOP_STORIES_URL = "https://news.ycombinator.com/"
FETCH_TIMEOUT = 3
NEWS_QUERY_SUBSTR = 'item?id='


class URLFetcher():
    """Provides counting of URL fetches for a particular task.
    """

    def __init__(self):
        self.fetch_counter = 0

    async def fetch(self, session, url):
        """Fetch a URL using aiohttp returning parsed JSON response.
        As suggested by the aiohttp docs we reuse the session.
        """
        with async_timeout.timeout(FETCH_TIMEOUT):
            async with session.get(url) as response:
                return await response.text()


def get_links(html, right='"', substr=None):
    new_urls = []
    for url in html.split('href="')[1:]:
        if substr:
            if substr in url and right in url:
                new_urls.append(url.split(right)[0])
        elif right in url:
            new_urls.append(url.split(right)[0])

    return new_urls


def get_top_news_links(links, limit):

    # удаление дубликатов
    unique_urls = set(links)
    urls = []
    for url in links:
        if url in unique_urls:
            urls.append(url)
            unique_urls.remove(url)

    if limit:
        urls = urls[:limit]

    return [urljoin(TOP_STORIES_URL, remove_fragment(new_url)) for new_url in urls]


def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url


async def saving(path, body):
    try:
        with open(path, 'wb') as f:
            f.write(body)
    except (PermissionError, IOError):
        pass


async def get_comments_page(loop, session, fetcher, link, save_dir, story=False):
    """Retrieve data for current post and recursively for all comments.
    """
    news_id = link.split(NEWS_QUERY_SUBSTR)[1]
    news_dir = os.path.abspath(os.path.join(save_dir, news_id))

    if not os.path.exists(news_dir):
        try:
            os.makedirs(news_dir)
        except (PermissionError, IOError) as e:
            tb_lines = traceback.format_exception(*sys.exc_info())
            logging.exception(''.join(tb_lines))
            raise e

        try:
            response = await fetcher.fetch(session, link)
        except Exception as e:
            logging.debug("Error retrieving post {}: {}".format(link, e))
            raise e

        try:
            await loop.run_in_executor(ThreadPoolExecutor, saving, news_dir, response)
        except Exception as e:
            logging.debug("Error retrieving saving new sites: {}".format(e))
            raise

        if story:
            links = get_links(response, right='" class="storylink"')
        else:
            links = get_links(response)

        # calculate this post's comments as number of comments
        number_of_comments = len(response['kids'])

        try:
            # create recursive tasks for all comments
            tasks = [asyncio.ensure_future(get_comments_page(
                loop, session, fetcher, link)) for link in links]

            # schedule the tasks and retrieve results
            try:
                results = await asyncio.gather(*tasks)
            except Exception as e:
                logging.debug("Error retrieving comments for top stories: {}".format(e))
                raise

            # reduce the descendents comments and add it to this post's
            number_of_comments += sum(results)
            logging.debug('{:^6} > {} comments'.format(post_id, number_of_comments))

            return number_of_comments
        except asyncio.CancelledError:
            if tasks:
                logging.info("Comments for post {} cancelled, cancelling {} child tasks".format(
                    post_id, len(tasks)))
                for task in tasks:
                    task.cancel()
            else:
                logging.info("Comments for post {} cancelled".format(post_id))
            raise


async def get_news(loop, session, limit, iteration, save_dir):
    """Retrieve top stories in HN.
    """
    fetcher = URLFetcher()  # create a new fetcher for this task
    try:
        response = await fetcher.fetch(session, TOP_STORIES_URL)
    except Exception as e:
        logging.error("Error retrieving top stories: {}".format(e))
        # return instead of re-raising as it will go unnoticed
        return
    except Exception as e:  # catch generic exceptions
        logging.error("Unexpected exception: {}".format(e))
        return

    # получение ссылок на страницы комментариев
    links = get_top_news_links(get_links(response, substr=NEWS_QUERY_SUBSTR), limit=limit)

    tasks = {
        asyncio.ensure_future(
            get_comments_page(loop, session, fetcher, link, save_dir, story=True)
        ): link for link in links}

    # return on first exception to cancel any pending tasks
    done, pending = await asyncio.shield(asyncio.wait(
        tasks.keys(), return_when=asyncio.FIRST_EXCEPTION))

    # if there are pending tasks is because there was an exception
    # cancel any pending tasks
    for pending_task in pending:
        pending_task.cancel()

    # process the done tasks
    for done_task in done:
        # if an exception is raised one of the Tasks will raise
        try:
            print("Post {} has {} comments ({})".format(
                tasks[done_task], done_task.result(), iteration))
        except Exception as e:
            print("Error retrieving comments for top stories: {}".format(e))

    return fetcher.fetch_counter


async def poll_top_news(loop, session, period, limit, save_dir):
    """Periodically poll for new stories and retrieve number of comments.
    """
    iteration = 1
    errors = []
    while True:
        if errors:
            logging.info('Error detected, quitting')
            return

        logging.info("Calculating comments for top {} stories. ({})".format(
            limit, iteration))

        future = asyncio.ensure_future(
            get_news(loop, session, limit, iteration, save_dir)
        )

        now = datetime.now()

        def callback(fut, errors):
            try:
                fetch_count = fut.result()
            except Exception as e:
                logging.debug('Adding {} to errors'.format(e))
                errors.append(e)
            except Exception as e:
                logging.exception('Unexpected error')
                errors.append(e)
            else:
                logging.info(
                    '> Download of news took {:.2f} seconds and {} fetches'.format(
                        (datetime.now() - now).total_seconds(), fetch_count))

        future.add_done_callback(partial(callback, errors=errors))

        logging.info("Waiting for {} seconds...".format(period))
        iteration += 1
        await asyncio.sleep(period)


async def run(loop, period, limit, save_dir):
    async with aiohttp.ClientSession(loop=loop) as session:
        await poll_top_news(loop, session, period, limit, save_dir)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--limit", action="store", default=30, type="int")
    op.add_option("-p", "--period", action="store", default=10, type="int")
    op.add_option("-d", "--downloads", action="store", default="./downloads/")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Crawler started.")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, opts.period, opts.limit, opts.downloads))
    loop.close()