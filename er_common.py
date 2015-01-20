from EventRegistry import EventRegistry, QueryArticles, RequestArticlesInfo
from EventRegistry import createStructFromDict
from datetime import date
from tweet_common import url_fix


def er_get_urls(start=date(2014, 4, 16), end=date(2014, 4, 16)):
    '''Get ER article URIs between a given interval of time.

    Returns:
        A dictionary of URL -> EventId (EventUri)
    '''

    er = EventRegistry(host="http://eventregistry.org", logging=False)

    page = 0
    urlmap = dict()

    while True:
        # setup query
        q = QueryArticles()
        q.setDateLimit(start, end)
        q.addRequestedResult(RequestArticlesInfo(includeBody=False,
                                                 includeTitle=False,
                                                 includeBasicInfo=False,
                                                 includeSourceInfo=False,
                                                 count=200, page=page))
        # make query
        res = er.execQuery(q)
        obj = createStructFromDict(res)

        # check if empty
        if len(obj.articles.results) == 0:
            return urlmap

        # add to dict of URI -> Event
        for article in obj.articles.results:
            if hasattr(article, 'eventUri'):
                urlmap[url_fix(article.uri)] = article.eventUri

        page += 1

    # unreachable
    return urlmap
