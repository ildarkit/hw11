import os
import sys
import asyncio
import logging
import traceback
from datetime import datetime
from functools import partial
from optparse import OptionParser
from urllib.parse import urljoin, urldefrag

import aiohttp
import async_timeout


TOP_STORIES_URL = "https://news.ycombinator.com/"
FETCH_TIMEOUT = 30
NEWS_QUERY_SUBSTR = 'item?id='


class URLFetcher():
    """Provides counting of URL fetches for a particular task.
    """

    def __init__(self):
        self.fetch_counter = 0

    async def fetch(self, session, url):
        """Fetch a URL using aiohttp returning content and status.
        """
        with async_timeout.timeout(FETCH_TIMEOUT):
            self.fetch_counter += 1
            async with session.get(url, ssl=False) as response:
                content = await response.read()
                return content, response.status, response.charset


def get_links(html, right='"', substr=None, split_pair=()):
    new_urls = []
    if split_pair:
        try:
            html = html.split(split_pair[0], maxsplit=1)[1]
            html = html.split(split_pair[1])[:-1]
        except IndexError:
            return new_urls
    html = ''.join(html)
    parts = html.split('href="')[1:]
    for part in parts:
        part = part.replace('&#x2F;', '/')
        if substr:
            if right in part:
                part = part.split(right)[0]
                if substr in part:
                    new_urls.append(part)
        elif right in part:
            new_urls.append(part.split(right)[0])

    return new_urls


def get_full_links(links, root, limit):

    # удаление дубликатов
    unique_urls = set(links)
    urls = []
    for url in links:
        if url in unique_urls:
            urls.append(url)
            unique_urls.remove(url)

    if limit:
        urls = urls[:limit]

    return [urljoin(root, remove_fragment(new_url)) for new_url in urls]


def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url


def writing(path, body):
    with open(path, 'wb') as f:
        f.write(body)


async def download(loop, session, fetcher, link, path):

    try:
        response, status, _ = await fetcher.fetch(session, link)
    except Exception as e:
        logging.debug("Error retrieving post {}: {}".format(link, e))
        raise

    if status == 200:

        path = os.path.join(path, link.replace('://', '_').replace('/', '_').replace('?', '_'))
        try:
            await loop.run_in_executor(None, writing, path, response)
        except (PermissionError, IOError) as e:
            logging.exception("Saving error: {}".format(e))
            raise


async def get_comments_urls(loop, session, fetcher, link, save_dir, story=False):
    """Retrieve data for current post.
    """
    try:
        response, status, charset = await fetcher.fetch(session, link)
    except Exception as e:
        logging.debug("Error retrieving post {}: {}".format(link, e))
        raise e

    if status != 200:
        logging.info(
            'Download url {} failed with status code {}.'.format(link, status)
        )
        return 0

    if charset:
        response = response.decode(charset)
    else:
        response = response.decode()

    news_id = link.split(NEWS_QUERY_SUBSTR)[1]
    save_dir = os.path.abspath(os.path.join(save_dir, news_id))
    links = []

    if not os.path.exists(save_dir):
        try:
            os.makedirs(save_dir)
        except (PermissionError, IOError):
            return 0

        if story:
            links = get_links(response, right='" class="storylink"', substr='http')

        comments_links = get_links(response, substr='http', split_pair=('comment-tree', '</table></td></tr>'))
        links.extend(comments_links)

        tasks = [asyncio.ensure_future(
            download(loop, session, fetcher, link, save_dir)
        ) for link in links]

        # schedule the tasks
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logging.debug("Error retrieving comments for top stories: {}".format(e))
            raise

    return len(links)


async def get_news(loop, session, limit, iteration, save_dir):
    """Retrieve top stories in HN.
    """
    fetcher = URLFetcher()  # create a new fetcher for this task
    try:
        response, status, charset = await fetcher.fetch(session, TOP_STORIES_URL)
    except Exception as e:
        logging.error("Error retrieving top stories: {}".format(e))
        # return instead of re-raising as it will go unnoticed
        return 0

    if status != 200:
        logging.info(
            'Download url {} failed with status code {}.'.format(
                TOP_STORIES_URL, status)
        )
        return 0

    if charset:
        response = response.decode(charset)
    else:
        response = response.decode()

    # получение ссылок на страницы комментариев
    links = get_links(response, substr=NEWS_QUERY_SUBSTR)
    links = get_full_links(
        links,
        TOP_STORIES_URL, limit=limit
    )

    tasks = {
        asyncio.ensure_future(
            get_comments_urls(
                loop, session, fetcher, link, save_dir, story=True
            )): link for link in links
    }

    done, _ = await asyncio.shield(asyncio.wait(tasks.keys()))

    # process the done tasks
    for done_task in done:
        # if an exception is raised one of the Tasks will raise
        logging.info("{} links have been downloaded from {}. ({})".format(
            done_task.result(), tasks[done_task], iteration))

    return fetcher.fetch_counter


async def poll_top_news(loop, session, period, limit, save_dir):
    """Periodically poll for new stories and retrieve number of comments.
    """
    iteration = 1
    while True:

        logging.info("Top {} news processing. ({})".format(
            limit, iteration))

        future = asyncio.ensure_future(
            get_news(loop, session, limit, iteration, save_dir)
        )

        now = datetime.now()

        def callback(fut):
            try:
                fetch_count = fut.result()
            except Exception as e:
                tb_lines = traceback.format_exception(*sys.exc_info())
                logging.exception(''.join(tb_lines))
            else:
                logging.info(
                    '> Download of news took {:.2f} seconds and {} fetches'.format(
                        (datetime.now() - now).total_seconds(), fetch_count))

        future.add_done_callback(partial(callback))

        logging.info("Waiting for {} seconds...".format(period))
        iteration += 1
        await asyncio.sleep(period)


async def run(loop, period, limit, save_dir):
    async with aiohttp.ClientSession(loop=loop) as session:
        await poll_top_news(loop, session, period, limit, save_dir)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-L", "--log", action="store", default="")
    op.add_option("-l", "--limit", action="store", default=30, type="int")
    op.add_option("-p", "--period", action="store", default=10, type="int")
    op.add_option("-d", "--downloads", action="store", default='./downloads/')
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Crawler started.")

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(loop, opts.period, opts.limit, opts.downloads))
    except KeyboardInterrupt:
        pass
    loop.close()