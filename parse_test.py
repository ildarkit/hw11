import asyncio
from urllib.parse import urljoin, urldefrag

import aiohttp


TOP_STORIES_URL = "https://news.ycombinator.com/"
TOP_STORIES_COUNT = 30


def remove_fragment(url):
    pure_url, frag = urldefrag(url)
    return pure_url


async def get_response():
    async with aiohttp.ClientSession() as session:
        async with session.get(TOP_STORIES_URL) as resp:
            print(resp.status)
            response = await resp.text()
            new_urls = [url.split('"')[0] for url in str(response).split('href="')[1:] if 'item?id=' in url]
            unique_urls = set(new_urls)
            urls = []
            for url in new_urls:
                if url in unique_urls:
                    urls.append(url)
                    unique_urls.remove(url)
            print([urljoin(TOP_STORIES_URL, remove_fragment(new_url)) for new_url in urls[:TOP_STORIES_COUNT]])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_response())
    loop.close()