#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
import time
from datetime import datetime, timezone
import pywikibot
from db_credentials import db_config

pywikibot.config.user_agent = "en:User:GalaxyBot (dreamrimmer.wikimedian@gmail.com)"

def query(interval):
    return f"""
    SELECT
        ROW_NUMBER() OVER (ORDER BY COUNT(rc_id) DESC) AS Rank,
        COUNT(rc_id) AS Reviews,
        actor_name AS Reviewer,
        SUM(comment_text LIKE "%Publishing accepted%") AS "Accept",
        SUM(comment_text LIKE "Declining submission:%") AS "Decline",
        SUM(comment_text LIKE "Commenting on submission%") AS "Comment",
        SUM(comment_text LIKE "Rejecting submission:%") AS "Reject",
        CONCAT(ROUND(SUM(comment_text LIKE "%Publishing accepted%") * 100 / COUNT(rc_id), 1), "%") AS "Accept %",
        CONCAT(ROUND(SUM(comment_text LIKE "Declining submission:%") * 100 / COUNT(rc_id), 1), "%") AS "Decline %",
        CONCAT(ROUND(SUM(comment_text LIKE "Commenting on submission%") * 100 / COUNT(rc_id), 1), "%") AS "Comment %",
        CONCAT(ROUND(SUM(comment_text LIKE "Rejecting submission:%") * 100 / COUNT(rc_id), 1), "%") AS "Reject %"
    FROM
        recentchanges_userindex
    LEFT JOIN
        actor ON rc_actor = actor_id
    LEFT JOIN
        comment ON rc_comment_id = comment_id
    WHERE
        (rc_namespace = 118 OR rc_namespace = 2)
        AND rc_type < 5
        AND (comment_text LIKE "Declining submission:%"
             OR comment_text LIKE "Rejecting submission:%"
             OR comment_text LIKE "Commenting on submission%"
             OR comment_text LIKE "%Publishing accepted%")
        AND rc_timestamp >= NOW() - INTERVAL {interval}
    GROUP BY
        rc_actor
    ORDER BY
        Reviews DESC
    LIMIT 100;
    """

def format_table(rows, headers):
    table = '{| class="wikitable sortable"\n'
    table += '|-\n' + ''.join(f'! {header}\n' for header in headers)
    for row in rows:
        table += '|-\n'
        for i, cell in enumerate(row):
            if headers[i] == "Reviewer":
                table += f'| [[User:{cell}|{cell}]]\n'
            else:
                table += f'| {cell}\n'
    table += '|}'
    return table

def fetch(query):
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        charset=db_config['charset']
    )
    with connection.cursor() as cursor:
        start_time = time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        execution_time = time.time() - start_time
    connection.close()
    decoded_rows = [[cell.decode('utf-8') if isinstance(cell, bytes) else cell for cell in row] for row in rows]
    return decoded_rows, headers, execution_time

def main():
    intervals = {
        "Last 24 hours": "1 DAY",
        "Last 7 days": "7 DAY",
        "Last 1 month": "30 DAY"
    }

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%H:%M, %d %B %Y (UTC)")
    bot_username = pywikibot.Site().username()
    content = "__TOC__\n\n"
    content += f"<div style='font-size:24px'>Top AfC reviewers as of {timestamp}</div>\n"
    content += f"Updated by [[User:{bot_username}]] ([[User talk:{bot_username}|talk]]) {timestamp}\n\n"

    for title, interval in intervals.items():
        rows, headers, time_taken = fetch(query(interval))
        content += f"== {title} ==\nQuery runtime: {time_taken:.2f} s\n"
        content += format_table(rows, headers) + "\n\n"

    print(content)

    site = pywikibot.Site("en", "wikipedia")
    page = pywikibot.Page(site, "User:GalaxyBot/Reports/Top AfC reviewers")

    page.text = content
    page.save("update AfC reports (bot)", bot=True, minor=False)

if __name__ == "__main__":
    main()
