import asyncio
import logging
from datetime import datetime
from functools import partial
from optparse import OptionParser

import aiohttp
import async_timeout


TOP_STORIES_COUNT = 30
URL_TEMPLATE = "https://news.ycombinator.com/item?id={}"
TOP_STORIES_URL = "https://news.ycombinator.com/"
FETCH_TIMEOUT = 10


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


async def post_number_of_comments(loop, session, fetcher, post_id):
    """Retrieve data for current post and recursively for all comments.
    """
    url = URL_TEMPLATE.format(post_id)
    try:
        response = await fetcher.fetch(session, url)
    except Exception as e:
        logging.debug("Error retrieving post {}: {}".format(post_id, e))
        raise e

    # base case, there are no comments
    if response is None or 'kids' not in response:
        return 0

    # calculate this post's comments as number of comments
    number_of_comments = len(response['kids'])

    try:
        # create recursive tasks for all comments
        tasks = [asyncio.ensure_future(post_number_of_comments(
            loop, session, fetcher, kid_id)) for kid_id in response['kids']]

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


async def get_comments_of_top_stories(loop, session, limit, iteration):
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

    tasks = {
        asyncio.ensure_future(
            post_number_of_comments(loop, session, fetcher, post_id)
        ): post_id for post_id in response[:limit]}

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


async def poll_top_stories_for_comments(loop, session, period, limit):
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
            get_comments_of_top_stories(loop, session, limit, iteration))

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
                    '> Calculating comments took {:.2f} seconds and {} fetches'.format(
                        (datetime.now() - now).total_seconds(), fetch_count))

        future.add_done_callback(partial(callback, errors=errors))

        logging.info("Waiting for {} seconds...".format(period))
        iteration += 1
        await asyncio.sleep(period)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--limit", action="store", default=30, type="int")
    op.add_option("-p", "--period", action="store", default=10, type="int")
    op.add_option("-d", "--downloads", action="store", default="/downloads/")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Crawler started.")

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop) as session:
        loop.run_until_complete(
            poll_top_stories_for_comments(
                loop, session, args.period, args.limit))
    loop.close()