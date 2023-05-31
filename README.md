## ⚠️ This code is unoptimized and needs contributors. [Read below](#drawbacks)

# webscraper
Scrape the web using SQLite and requests_html! 

## Usage
**Warning:** Using this script may get you blocked from certain websites, use at your own risk.
```sh
pip3 install -r requirements.txt
python main.py
```

## Data used
The SQLite database is stored at `dev.db`.
It has two tables, `TO_CHECK` AND `DATA`.

The `TO_CHECK` table stores the pages that the scraper has to check.
It has three columns, `id` (the ID), `uri` (the page to check) and `sitemap` (1 if it is a [sitemap](https://developers.google.com/search/docs/crawling-indexing/sitemaps/overview) to check or 0 if it is a webpage)

The `DATA` table stores the pages the scraper has already checked.
Its columns are
- `id` (The ID)
- `uri` (The URI of the page)
- `error` (1 if there was an error in getting the page)
- `type` (IOError if there was an error, else the MIME type of the page or NULL)
- `title` (The title of the page if any)
- `description` (The description of the page if any)
- `keywords` (A JSON list of keywords)
- `linksto` (A list of absolute links that the page links to)
- `createdAt` (A timestamp at which the page was indexed)

## Drawbacks
- The script is fairly unoptimized
- It doesn't run JavaScript so most major social media sites contain usable data
- It doesn't have a proper indexing system and doesn't have a frontend to search the data used.

## Contributions
As you can see from the drawbacks above, the script is in real use of help. If you can find the tiniest thing that would improve the script, please feel free to contribute.
