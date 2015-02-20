import sys
import time
import socket
import json

from EventRegistry import EventRegistry, QueryArticles, RequestArticlesInfo
from EventRegistry import QueryEvent, RequestEventArticles
from EventRegistry import RequestArticlesIdList
from EventRegistry import RequestEventArticleUris, createStructFromDict
from datetime import date

from tweet_common import url_fix
from ermcfg import SOCKET_TIMEOUT, REQUEST_SLEEP, ER_LOG, ER_USER, ER_PASS
from ermcfg import ARTICLES_BATCH_SIZE, EVENTS_BATCH_SIZE, URLS_PER_PAGE
from ermcfg import ER_WAIT_BETWEEN_REQUESTS


""" Deprecated
def er_get_urls(start=date(2014, 4, 16), end=date(2014, 4, 16)):
    '''Get ER article URIs between a given interval of time.

    Returns:
        A dictionary of URL -> EventId (EventUri)
    '''

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    er = EventRegistry(host="http://eventregistry.org", logging=ER_LOG)
    er.login(ER_USER, ER_PASS)

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
                                                 count=100, page=page))
        # make query
        try:
            res = er.execQuery(q)
        except socket.timeout:
            e = sys.exc_info()[0]
            print(e)
            time.sleep(REQUEST_SLEEP)
            continue  # retry

        if u'error' in res:
            raise ValueError('EventRegistry API Return Error')

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
"""


def er_execute_query(er, q, n_retries=1000, wait=REQUEST_SLEEP,
                     wait_before=ER_WAIT_BETWEEN_REQUESTS):
    '''Makes the query to ER, handles timeouts, retries after a period and ER
    API errors. Also waits be
    '''

    # Give ER time to breath between requests
    if wait_before > 0:
        time.sleep(wait_before)

    counter = 0
    while True:
        try:
            res = er.execQuery(q)
            break
        except socket.timeout:
            e = sys.exc_info()[0]
            print(e)
            time.sleep(wait)
            if counter > n_retries:
                raise
            counter += 1
            # retry

    # error handling
    if u'error' in res:
        raise ValueError('EventRegistry API Return Error' + res['error'])

    return res


def er_get_urls_for_day(day=date(2014, 4, 16), lang='eng'):
    '''Get ER article URIs between a given day

    Returns:
        A dictionary of URL -> EventId (EventUri)
    '''

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    er = EventRegistry(host="http://eventregistry.org", logging=ER_LOG)
    er.login(ER_USER, ER_PASS)

    page = 0
    total_urls = 0
    urlmap = dict()

    # setup query
    q = QueryArticles(lang=lang)
    q.setDateLimit(day, day)
    # request article id list
    q.addRequestedResult(RequestArticlesIdList())

    # make query
    print('Fetching URLs for day %s' % (str(day),))
    res = er_execute_query(er, q)

    articleIds = res["articleIds"]

    for articleId in range(0, len(articleIds), URLS_PER_PAGE):
        # setup query
        chunk = articleIds[articleId:articleId + URLS_PER_PAGE]
        q = QueryArticles()
        q.setArticleIdList(chunk)
        q.addRequestedResult(RequestArticlesInfo(includeBody=False,
                                                 includeTitle=False,
                                                 includeBasicInfo=False,
                                                 includeSourceInfo=False,
                                                 page=0, count=URLS_PER_PAGE))
        page_urls = 0

        # make query
        res = er_execute_query(er, q)
        obj = createStructFromDict(res)

        # check if empty
        if len(obj.articles.results) == 0:
            print('Fetched a total of %d urls', (total_urls,))
            return urlmap

        # add to dict of URI -> Event
        for article in obj.articles.results:
            if hasattr(article, 'eventUri'):
                urlmap[url_fix(article.uri)] = article.eventUri
                page_urls += 1

        #print('Fetched page %d: %d urls' % (page, page_urls))
        total_urls += page_urls
        page += 1
    # unreachable

    return urlmap


def er_get_events_article(event_ids, lang='eng'):
    '''
    Gets centroid article for a list of events
    '''

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    er = EventRegistry(host="http://eventregistry.org", logging=ER_LOG)
    er.login(ER_USER, ER_PASS)

    artmap = dict()
    batch_size = ARTICLES_BATCH_SIZE
    n_events = len(event_ids)

    print('Fetching articles for %d events' % (n_events,))

    for ii in range(0, n_events, batch_size):
        batch_ids = event_ids[ii:ii + batch_size]

        page_centroids = 0
        page_events = 0

        q = QueryEvent(batch_ids)
        q.addRequestedResult(RequestEventArticles(count=2,
                                                  lang=lang, bodyLen=-1))
        res = er_execute_query(er, q)

        events = res.keys()
        page_events = len(events)
        if page_events == 0:
            break

        for eventid in events:
            info = res[eventid]['articles']['results']
            if len(info) == 0:
                continue

            page_centroids += 1
            info = info[0]
            a = json.dumps({'body': info['body'], 'title': info['title']})
            artmap[eventid] = a

            #print('Fetched %d centroids / %d' % (page_centroids, page_events))

    return artmap


def er_get_events_urls(event_ids, lang='eng'):
    '''
    Gets article urls for a list of events
    Used in "online" mode
    '''

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    er = EventRegistry(host="http://eventregistry.org", logging=ER_LOG)
    er.login(ER_USER, ER_PASS)

    urlmap = dict()
    batch_size = EVENTS_BATCH_SIZE

    n_events = len(event_ids)

    print('Fetching urls for %d events' % (n_events,))
    for ii in range(0, len(event_ids), batch_size):
        batch_ids = event_ids[ii:ii + batch_size]
        page = 0
        while True:
            page_urls = 0

            q = QueryEvent(batch_ids)
            q.addRequestedResult(RequestEventArticleUris())
            res = er_execute_query(er, q)

            events = res.keys()
            page_events = len(events)
            if page_events == 0:
                break

            for eventid in events:
                for url in res[eventid]:
                    urlmap[url] = eventid
                    page_urls += 1

            print('Fetched page %d: %d urls / %d events' % (page, page_urls,
                                                            page_events))
            page += 1

    return urlmap


def er_get_latest(lang='eng'):
    '''
    Generator: returns (urlmap, artmap, datemap), respectively:
        url-eventid
        eventid-articleInfo: {'title', 'body'}
        date-[eventid1, ...]
    '''
    lastActivityId = 0
    er = EventRegistry(host="http://eventregistry.org", logging=ER_LOG)
    er.login(ER_USER, ER_PASS)

    while True:
        # get events that have recently changed
        # first time (when lastActivityIdId==0) this will get you max
        # 1000 events most recently changed
        # (if they were not updated more than 60 minutes in the past)
        # on following calls, by specifying lastActivityIdId this will only
        # return events changed after your last request

        eventsDict = er.getRecentEvents(100, 60, lang=lang,
                                        eventsWithLocationOnly=False,
                                        lastStoryActivityId=lastActivityId)
        # if ER is offline or you get a timeout then you might get None as the
        # result

        # Check if the response from the server was an error message
        if u'error' in eventsDict:
            raise ValueError('EventRegistry API Return Error'
                             + eventsDict['error'])

        if eventsDict is not None:
            events = eventsDict['recentActivity']['events']['activity']
            size = EVENTS_BATCH_SIZE

            for i in range(0, len(events), size):
                event_ids = events[i:i + size]
                artmap = er_get_events_article(event_ids, lang='eng')
                urlmap = er_get_events_urls(event_ids, lang=lang)

            datemap = dict()
            info = eventsDict['recentActivity']['events']['eventInfo']
            for event_id in events:
                date = str(info[event_id]['eventDate'])
                if date not in datemap:
                    datemap[date] = [str(event_id)]
                else:
                    datemap[date].append(str(event_id))

            new_id = eventsDict['recentActivity']['events']['lastActivityId']
            # if ER rebooted you might get activity id that is smaller than
            # the last one you have
            if new_id < lastActivityId:
                lastActivityId = 0
            else:
                # remember the last updated id
                lastActivityId = new_id

        # --- yield (return) ---
        yield (urlmap, artmap, datemap)
        # --- next entry point ---

        # sleep for 60 seconds
        time.sleep(60)
