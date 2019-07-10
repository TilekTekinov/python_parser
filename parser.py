import csv
import requests

from datetime import datetime
from bs4 import BeautifulSoup


# get proxies from file with generator
def get_proxies(file_name):
    # read proxy, need format like:
    # host;ip;port;lastseen;delay;cid;country_code;country_name;city;checks_up;checks_down;anon;http;ssl;socks4;socks5
    # from - https://hidemyna.me/en/proxy-list/#list    code: 65600464320741
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_NONE)
        for row in reader:
            yield row


# get response from url with proxy
def get_html(url, proxies):
    user_agent = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
    proxies = next(proxies)
    if proxies[-4]:
        schema = 'http'
    else:
        schema = 'https'
    proxy_ip_port = schema + '://' + proxies[0] + ':' + proxies[2]  # http://177.81.25.228:4145 or https://177.81.25.228:4145
    proxy = {schema: str(proxy_ip_port)}                            # {'http': 'http://177.81.25.228:4145'} or {'https': 'http://177.81.25.228:4145'}
    r = requests.get(url, headers=user_agent, proxies=proxy, timeout=(((int(proxies[4])/1000)*2 + 5), ((int(proxies[4])/1000)*4 + 5)))
    return r, proxies


# get category link and name
def get_category_name(html):
    soup = BeautifulSoup(html, "html.parser")
    category = soup.find_all(attrs={'class': 'category-top'})
    for category_one in category:
        data = {'category-link': category_one.get('href'),
                'category-name': category_one.getText().strip().split('->')[0],
                'c': '',
                'd': ''}
        write_csv(data, 'category-link', 'category-name', 'c', 'd', 'category.csv')


# get category with proxy and catch Exception
def get_category_top(category_name_url):
    while True:
        try:
            proxies = get_proxies('hideme_proxy.csv')
            category_name_r, proxy = get_html(category_name_url, proxies)
            category_name_r.raise_for_status()
            if 'Bad Request' in category_name_r.text:
                raise requests.HTTPError
            get_category_name(category_name_r.text)
            break
        except requests.HTTPError as http_err:
            write_log('HTTP error occurred', http_err, category_name_url)
            if category_name_r.status_code == 403:
                write_log('403 error because of proxy', proxy, category_name_url)
            elif category_name_r.status_code == 404:
                write_log('404 error because of proxy', proxy, category_name_url)
            continue
        except requests.ConnectionError as conn_err:
            write_log('Connection error occurred', conn_err, category_name_url)
            continue
        except requests.exceptions.ReadTimeout as time_err:
            write_log('Time error occurred', time_err, category_name_url)
            continue


# get subcategories link and name
def get_category_name_products(html, category_top):
    soup = BeautifulSoup(html, "html.parser")
    category = soup.find_all(attrs={'class': 'category-products'})
    for category_one in category:
        data = {'category-top': category_top,
                'category-link': category_one.get('href'),
                'category-name': category_one.getText().strip().replace(u'\xa0', u',').split(',')[1],
                'd': ''}
        write_csv(data, 'category-top', 'category-link', 'category-name', 'd', 'category-prod.csv')


# get subcategories with proxy and catch Exception
def get_category_products():
    category_top = get_csv('category.csv')
    proxies = get_proxies('hideme_proxy.csv')
    i = 0
    while True:
        print('i: ', i)
        try:
            if i == len(category_top) - 1:
                break
            category_top_r, proxy = get_html(category_top[i][0], proxies)
            category_top_r.raise_for_status()
            if 'Bad Request' in category_top_r.text:
                raise requests.HTTPError
            get_category_name_products(category_top_r.text, category_top[i][0])
            i += 1
        except requests.HTTPError as http_err:
            if category_top_r.status_code == 403:
                write_log('403 error because of proxy', proxy, category_top[i][0])
            elif category_top_r.status_code == 404:
                write_log('404 error because of proxy', proxy, category_top[i][0])
            elif 'Bad Request' in category_top_r.text:
                write_log('Bad Request', category_top_r.text, category_top[i][0])
            write_log('HTTP error occurred', http_err, category_top[i][0])
            continue
        except requests.ConnectionError as conn_err:
            write_log('Connection error occurred', conn_err, category_top[i][0])
            continue
        except requests.exceptions.ReadTimeout as time_err:
            write_log('Time error occurred', time_err, category_top[i][0])
            continue


# read from file
def get_csv(file):
    data = []
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter=';', quotechar='"')
        for row in reader:
            data.append(row)
    return data


# write to file
def write_csv(data, a, b, c, d, file):
    with open(file, 'a') as name:
        writer = csv.writer(name, delimiter=';')
        writer.writerow((data[a],
                         data[b],
                         data[c],
                         data[d]))


# write log to file
def write_log(text, err, link):
    with open('log.csv', 'a') as log:
        writer = csv.writer(log, delimiter=';')
        writer.writerow((datetime.now(), text, err, link))


def main():
    start = datetime.now()
    print('Start: ', start)

    # category_name_url = 'http://www.i28.com/jordan-%E4%B9%94%E4%B8%B9-c-995/'
    # get_category_top(category_name_url)

    get_category_products()

    end = datetime.now()
    print('Total: ', str(end - start))


if __name__ == '__main__':
    main()
