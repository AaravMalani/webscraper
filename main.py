import bs4
from urllib.parse import urlparse
import requests
import requests_html
import sqlite3
import time
import threading
import os
import gzip
import json
session = requests_html.HTMLSession()
# connect to the database and allow multithreading
db = sqlite3.connect("dev.db", check_same_thread=False)

# use the WAL journaling mode which is a bit faster
db.execute("PRAGMA journal_mode=WAL;")

# use a larger cache size
db.execute("PRAGMA cache_size=-1048576;")

# create the table for things to check
db.execute("CREATE TABLE IF NOT EXISTS TO_CHECK (id INTEGER PRIMARY KEY AUTOINCREMENT, uri TEXT, sitemap BOOLEAN);")

# create the table for checked data
db.execute("CREATE TABLE IF NOT EXISTS DATA (id INTEGER PRIMARY KEY AUTOINCREMENT, uri TEXT, error BOOLEAN, type TEXT, title TEXT, description TEXT, keywords TEXT, linksto TEXT, createdAt TIMESTAMP DEFAULT (DATETIME('now')));")

# save data every 60s


def save():
    while True:
        time.sleep(60)
        db.commit()
        print("\x1b[1;32mSaved!\x1b[0m")


# data we are currently checking in the threads
curr_checking = set()


def searcher_sitemaps(target):
    global curr_checking

    # print the thing we are checking
    print(target)
    try:
        # validate the link
        link = urlparse(target)
    except:
        return
    # make sure the link is completely valid
    if not link or link.scheme not in ['http', 'https']:
        return
    # add to the curr_checking set
    curr_checking.add(target)
    try:
        # get the data (perhaps use a HEAD request first then GET?)
        data = requests.get(target)
    except:
        # add error to db
        db.execute("INSERT INTO DATA (uri, error, type) VALUES (?, ?, ?)",
                   (target, True, "IOError"))
        try:
            # we are no longer checking
            curr_checking.remove(target)
        except:
            pass
        return
    # status code
    if not data.ok:
        # add error to db
        db.execute("INSERT INTO DATA (uri, error, type) VALUES (?, ?, ?)",
                   (target, True, "SC"+str(data.status_code)))
        try:
            # we are no longer checking this link
            curr_checking.remove(target)
        except:
            pass
        return
    # if this is not html, then do not save
    # (if this is html, we want to still check it using `searcher`, but if its some other file format like XML
    #  then we should make sure that we have proof that we have indexed it)
    if data.headers.get('Content-Type', '').split(';')[0].lower() != 'text/html':
        db.execute("INSERT INTO DATA (uri, error, type) VALUES (?, ?, ?)", (target,
                   False, data.headers.get('Content-Type', '').split(';')[0].lower()))

    # if this isn't XML/GZipped data, then we cannot index it as a sitemap
    if data.headers.get('Content-Type', '').split(';')[0].lower() not in ['application/xml', 'text/xml', 'application/gzip']:
        try:
            curr_checking.remove(target)
        except:
            pass
        return

    # If this is GZip, then we need to decompress it
    if data.headers.get('Content-Type', '').split(';')[0].lower() == 'application/gzip':
        soup = bs4.BeautifulSoup(gzip.decompress(data.content), "xml")
    else:
        soup = bs4.BeautifulSoup(data.text, "xml")
    sitemaps = set()
    # get the sitemaps in file
    try:
        sitemaps = set(j.text for i in soup.find_all("sitemap")
                       for j in i.find_all("loc")) - curr_checking
        # remove sitemaps that we plan to check
        if sitemaps:
            sitemaps -= set(i[0] for i in db.execute("SELECT uri FROM TO_CHECK WHERE uri IN (" +
                                                     (", ".join(["?"]*len(sitemaps)))+")", tuple(sitemaps)).fetchall())
        # remove sitemaps that we have already checked
        if sitemaps:
            sitemaps -= set(i[0] for i in db.execute("SELECT uri FROM DATA WHERE uri IN (" +
                                                     (", ".join(["?"]*len(sitemaps)))+")", tuple(sitemaps)).fetchall())

    except:
        # there were no sitemaps
        sitemaps = set()

    # convert the sitemaps into a format used by SQLite
    sitemaps2 = [(i, True) for i in sitemaps]
    try:
        # join the url.locs and url.xhtml:links
        links = set.union(set(j.text for i in soup.find_all("url") for j in i.findall("loc")), set(
            j.attrs['html'] for i in soup.find_all("xhtml:link", {"href": True}) for j in i.findall("loc"))) - curr_checking
        # remove links we plan to check
        if links:
            links -= set(i[0] for i in db.execute("SELECT uri FROM TO_CHECK WHERE uri IN ("+(
                ", ".join(["?"]*len(links)))+")", tuple(links)).fetchall())
        # remove links we have already checked
        if links:
            links -= set(i[0] for i in db.execute("SELECT uri FROM DATA WHERE uri IN (" +
                         (", ".join(["?"]*len(links)))+")", tuple(links)).fetchall())
        # convert into a format used by SQLite and join with sitemaps
        links = list(set(sitemaps2 + [(i, False) for i in links]))
    except:
        # there were no links
        links = sitemaps2

    # if there are any links or sitemaps
    if links:
        # we plan to check the links and sitemaps (True: sitemap, False: link)
        db.execute("INSERT INTO TO_CHECK (uri, sitemap) VALUES " +
                   (", ".join(["(?, ?)"] * len(links))), [x for i in links for x in i])
    try:
        # we are no longer checking this sitemap
        curr_checking.remove(target)
    except:
        pass


def searcher(target):
    global curr_checking
    # print the target we are crawling
    print(target)
    try:
        # try to parse the link
        link = urlparse(target)
    except:
        # invalid link
        return
    # validate link
    if not link or link.scheme not in ['http', 'https']:
        return
    # we are checking this link
    curr_checking.add(target)

    try:
        # request this link
        data = session.get(target)

    except:
        # we errored out!
        db.execute("INSERT INTO DATA (uri, error, type) VALUES (?, ?, ?)",
                   (target, True, "IOError"))
        try:
            # we are no longer checking this link
            curr_checking.remove(target)
        except:
            pass
        return
        
    if data.headers.get('Content-Type', '').split(';')[0].lower() != 'text/html':
        # not HTML, so we can't crawl
        db.execute("INSERT INTO DATA (uri, error, type) VALUES (?, ?, ?)", (target,
                   False, data.headers.get('Content-Type', '').split(';')[0].lower()))
        try:
            # we are no longer checking this link
            curr_checking.remove(target)
        except:
            pass
        return

    links = set(data.html.absolute_links)
    sitemaps = set()

    if sitemaps:
        sitemaps -= set(i[0] for i in db.execute("SELECT uri FROM TO_CHECK WHERE uri IN (" +
                        (", ".join(["?"]*len(sitemaps)))+")", tuple(sitemaps)).fetchall())
    if sitemaps:
        sitemaps -= set(i[0] for i in db.execute("SELECT uri FROM DATA WHERE uri IN (" +
                        (", ".join(["?"]*len(sitemaps)))+")", tuple(sitemaps)).fetchall())
    sitemaps -= curr_checking
    sitemaps = [(i, True) for i in sitemaps]
    if links:
        links -= set(i[0] for i in db.execute("SELECT uri FROM TO_CHECK WHERE uri IN (" +
                     (", ".join(["?"]*len(links)))+")", tuple(links)).fetchall())
    if links:
        links -= set(i[0] for i in db.execute("SELECT uri FROM DATA WHERE uri IN (" +
                     (", ".join(["?"]*len(links)))+")", tuple(links)).fetchall())
    links -= curr_checking

    links = [(i, False) for i in links]
    links += sitemaps

    if links:
        db.execute("INSERT INTO TO_CHECK (uri, sitemap) VALUES " +
                   (", ".join(["(?, ?)"] * len(links))), [x for i in links for x in i])

    # to_check_lock.acquire()
    db.execute("INSERT INTO DATA (uri, error, type, title, description, keywords, linksto) VALUES (?, ?, ?, ?, ?, ?, ?)", (target, data.ok, data.headers.get('Content-Type', '').split(';')[0].lower() or None,
        (getattr(data.html.find('title'), 'text', None) or getattr(
            data.html.find('h1'), 'getText', lambda: "")())[:160],
        (getattr(data.html.find("meta[name=description]"), "attrs", {}).get(
            "content") or getattr(data.html.find("p"), "getText", lambda: "")())[:160],
        json.dumps([i.strip() for i in (getattr(data.html.find(
            "meta[name=keywords]"), "attrs", {}).get("content") or "").split(",") if i]),
        json.dumps(set(data.html.absolute_links))
    ))
    # to_check_lock.release()
    try:
        curr_checking.remove(target)
    except:
        pass


to_check_lock = threading.Lock()

if not db.execute("SELECT COUNT(id) FROM TO_CHECK").fetchone()[0]:
    db.execute('INSERT INTO TO_CHECK (uri, sitemap) VALUES (?, ?)',
               ('https://github.com', False))

threading.Thread(target=save, daemon=True).start()



try:

    while True:
        while threading.active_count() > os.cpu_count() * 64:
            pass
        # to_check_lock.acquire()
        thing=db.execute(
            "SELECT * FROM TO_CHECK ORDER BY RANDOM() LIMIT 64").fetchall()
        # print(thing)
        if thing:
            db.execute("DELETE FROM TO_CHECK WHERE id IN (" +
                       (", ".join(["?"]*len(thing)))+")", [i[0] for i in thing])
        # to_check_lock.release()

        for i in thing:
             threading.Thread(target=searcher if not i[2] else searcher_sitemaps, args=(
               i[1],), daemon=True).start()
except:
    db.commit()
    db.close()
