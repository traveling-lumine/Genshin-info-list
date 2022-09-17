import sqlite3
from time import sleep

import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse


class Row:
    def __init__(self, tag):
        self.href = int(tag['href'].split('/')[3].split('?')[0])
        self.col_id = int(tag.find(attrs={'class': 'col-id'}).text)
        self.title = list(tag.find(attrs={'class': 'title'}))[2].strip()
        self.writer = tag.find(attrs={'class': 'user-info'}).span['data-filter']
        self.time = isoparse(
            tag.find(attrs={'class': 'col-time'}).time['datetime'])
        self.view = int(tag.find(attrs={'class': 'col-view'}).text)
        self.rate = int(tag.find(attrs={'class': 'col-rate'}).text)
        try:
            self.comment = int(
                tag.find(attrs={'class': 'comment-count'}).text.strip('[]'))
        except AttributeError:
            self.comment = 0
        self.best = tag.find(attrs={'class': 'ion-android-star'}) is not None

    def __str__(self):
        return f'{self.href:8} {self.col_id:5} {"★" if self.best else ""} ' \
               f'{self.title:50} {self.comment} {self.writer:20} ' \
               f'{self.time.timestamp()} {self.view} {self.rate}'

    def add_db(self):
        try:
            cursor.execute(
                'INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (self.href, self.col_id, self.title, self.writer,
                 int(self.time.timestamp()), self.view, self.rate, self.comment,
                 self.best)
            )
        except sqlite3.IntegrityError as e:
            if 'UNIQUE constraint failed' not in str(e):
                raise e


def main():
    before = 99999999
    cont = True
    while cont:
        before, cont = crawl(before)
        print(before, cont)


def crawl(before):
    url = f'https://arca.live/b/genshin' \
          f'?category=%F0%9F%92%A1%EC%A0%95%EB%B3%B4' \
          f'&before={before}'
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.select("[class='vrow column']")
        title = [Row(tag) for tag in title]
        for row in title:
            row.add_db()
        connection.commit()
        sleep(1)
        return title[-1].href, len(title) > 0
    else:
        print(response.status_code)
        sleep(10)
        return before, True


if __name__ == '__main__':
    connection = sqlite3.connect('list.db')
    cursor = connection.cursor()
    cursor.execute(
        """
        create table if not exists articles
        (
            href    INTEGER not null
                constraint href_pk
                    primary key,
            col_id  INTEGER not null,
            title   TEXT    not null,
            writer  TEXT    not null,
            `time`    INTEGER not null,
            `view`    INTEGER not null,
            rate    INTEGER not null,
            comment INTEGER not null,
            best    INTEGER not null
        );
        """
    )
    main()
    cursor.close()
    connection.close()
