import csv
import math

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


# Start pars category ***********
# get category link and name
def get_category_link(html):
    soup = BeautifulSoup(html, "html.parser")
    category = soup.find_all(attrs={'class': 'category-top'})
    try:
        for category_one in category:
            data = {'category-link': category_one.get('href'),
                    'category-name': category_one.getText().strip().split('->')[0],
                    'c': '',
                    'd': ''}
    except AttributeError:
        data = {'category-link': '',
                'category-name': '',
                'c': '',
                'd': '',
                'e': '',
                'f': ''}
    write_csv(data, 'category-link', 'category-name', 'c', 'd', 'e', 'f', 'category-list-url.csv')


# get category with proxy and catch Exception
def get_category_links():
    base_url = 'http://www.i28.com/jordan-%E4%B9%94%E4%B8%B9-c-995/'
    proxies = get_proxies('hideme_proxy.csv')
    while True:
        try:
            resp, proxy = get_html(base_url, proxies)
            if 13000 >= int(resp.headers['Content-Length']):
                raise KeyError
            resp.raise_for_status()
            if 'Bad Request' in resp.text:
                raise requests.HTTPError
            get_category_link(resp.text)
            break
        except requests.HTTPError as http_err:
            if resp.status_code == 403:
                write_log('403 error because of proxy', proxy, base_url)
            elif resp.status_code == 404:
                write_log('404 error because of proxy', proxy, base_url)
            elif 'Bad Request' in resp.text:
                write_log('Bad Request', resp.text.strip(), base_url)
            write_log('HTTP error occurred', http_err, base_url)
            continue
        except requests.ConnectionError as conn_err:
            write_log('Connection error occurred', conn_err, base_url)
            continue
        except ConnectionResetError as res_err:
            write_log('ConnectionResetError error occurred', res_err, base_url)
            continue
        except requests.exceptions.ChunkedEncodingError as con_err:
            write_log('ChunkedEncodingError error occurred', con_err, base_url)
            continue
        except requests.exceptions.ReadTimeout as time_err:
            write_log('Time error occurred', time_err, base_url)
            continue
        except KeyError:
            write_log('Bad proxy', resp.headers, base_url)
            continue
# End pars category *************


# Start pars subcategories ************
# get subcategories link and name
def get_subcategory_link(html, category_link):
    soup = BeautifulSoup(html, "html.parser")
    try:
        category = soup.find_all(attrs={'class': 'category-products'})
        for category_one in category:
            data = {'category-link': category_link,
                    'subcategory-link': category_one.get('href'),
                    'subcategory-name': category_one.getText().strip().replace(u'\xa0', u',').split(',')[1],
                    'd': ''}
    except AttributeError:
        data = {'category-link': category_link,
                'subcategory-link': '',
                'subcategory-name': '',
                'd': '',
                'e': '',
                'f': ''}
    write_csv(data, 'category-top', 'subcategory-link', 'subcategory-name', 'd', 'e', 'f', 'subcategory-list-url.csv')


# get subcategories with proxy and catch Exception
def get_subcategory_links():
    category_link = get_csv('category-list-url.csv')
    proxies = get_proxies('hideme_proxy.csv')
    i = 0
    while True:
        try:
            if i == len(category_link) - 1:
                break
            resp, proxy = get_html(category_link[i][0], proxies)
            if 13000 >= int(resp.headers['Content-Length']):
                raise KeyError
            resp.raise_for_status()
            if 'Bad Request' in resp.text:
                raise requests.HTTPError
            get_subcategory_link(resp.text, category_link[i][0])
            i += 1
        except requests.HTTPError as http_err:
            if resp.status_code == 403:
                write_log('403 error because of proxy', proxy, category_link[i][0])
            elif resp.status_code == 404:
                write_log('404 error because of proxy', proxy, category_link[i][0])
            elif 'Bad Request' in resp.text:
                write_log('Bad Request', resp.text.strip(), category_link[i][0])
            write_log('HTTP error occurred', http_err, category_link[i][0])
            continue
        except requests.ConnectionError as conn_err:
            write_log('Connection error occurred', conn_err, category_link[i][0])
            continue
        except ConnectionResetError as res_err:
            write_log('ConnectionResetError error occurred', res_err, category_link[i][0])
            continue
        except requests.exceptions.ChunkedEncodingError as con_err:
            write_log('ChunkedEncodingError error occurred', con_err, category_link[i][0])
            continue
        except requests.exceptions.ReadTimeout as time_err:
            write_log('Time error occurred', time_err, category_link[i][0])
            continue
        except KeyError:
            write_log('Bad proxy', resp.headers, category_link[i][0])
            continue
# End pars subcategories **************


# Start pars all products from subcategories ****************
# get number of pages in subcategories
def get_page_count(subcategory_link):
    proxies = get_proxies('hideme_proxy.csv')
    while True:
        try:
            resp, proxy = get_html(subcategory_link + '?sort=2d&page=' + str(1), proxies)
            if 13000 >= int(resp.headers['Content-Length']):
                raise KeyError
            resp.raise_for_status()
            if 'Bad Request' in resp.text:
                raise requests.HTTPError
            soup = BeautifulSoup(resp.text, "html.parser")
            page_count = soup.find(attrs={'class': 'displaying hidden-xs'}).contents[5].contents
            return int(page_count[0])
        except requests.HTTPError as http_err:
            if resp.status_code == 403:
                write_log('403 error because of proxy', proxy, subcategory_link)
            elif resp.status_code == 404:
                write_log('404 error because of proxy', proxy, subcategory_link)
            elif 'Bad Request' in resp.text:
                write_log('Bad Request', resp.text.strip(), subcategory_link)
            write_log('HTTP error occurred', http_err, subcategory_link)
            continue
        except requests.ConnectionError as conn_err:
            write_log('Connection error occurred', conn_err, subcategory_link)
            continue
        except ConnectionResetError as res_err:
            write_log('ConnectionResetError error occurred', res_err, subcategory_link)
            continue
        except requests.exceptions.ChunkedEncodingError as con_err:
            write_log('ChunkedEncodingError error occurred', con_err, subcategory_link)
            continue
        except requests.exceptions.ReadTimeout as time_err:
            write_log('Time error occurred', time_err, subcategory_link)
            continue
        except KeyError:
            write_log('Bad proxy', resp.headers, subcategory_link)
            continue


# get product link and price
def get_product_link(html, subcategory_link, i):
    soup = BeautifulSoup(html, "html.parser")
    prod_info = soup.find_all(class_="product-col")
    for prod_info_one in prod_info:
        # try:
        #     name.append(prod_info_one.find(class_='title').getText())
        # except AttributeError:
        #     name.append('')
        try:
            link = prod_info_one.find(class_='title').find('a').get('href')
        except AttributeError:
            link = ''
        try:
            price = prod_info_one.find(class_='productBasePrice').getText()
        except AttributeError:
            try:
                price = prod_info_one.find(class_='productSpecialPrice').getText()
            except AttributeError:
                price = ''
        data = {'subcategory': subcategory_link + '?sort=2d&page=' + str(i),
                'link': link,
                'price': price,
                'd': '',
                'e': '',
                'f': ''}
        write_csv(data, 'subcategory', 'link', 'price', 'd', 'e', 'f', 'product_list_url.csv')


# get product from all subcategories page
def get_product_links(subcategory_link):
    all_page = get_page_count(subcategory_link)
    proxies = get_proxies('hideme_proxy.csv')
    sum_proxies = len(list(get_proxies('hideme_proxy.csv')))
    count = 0
    for i in range(1, math.ceil(int(all_page) / 24) + 1):
        while True:
            try:
                if count == sum_proxies - 1:
                    write_log('End of proxy file', 'started file from the beginning', '')
                    del proxies
                    proxies = get_proxies('hideme_proxy.csv')
                    count = 0
                resp, proxy = get_html(subcategory_link + '?sort=2d&page=' + str(i), proxies)
                if 13000 >= int(resp.headers['Content-Length']):
                    raise KeyError
                resp.raise_for_status()
                if 'Bad Request' in resp.text:
                    raise requests.HTTPError
                get_product_link(resp.text, subcategory_link, i)
                break
            except requests.HTTPError as http_err:
                if resp.status_code == 403:
                    write_log('403 error because of proxy', proxy, subcategory_link)
                elif resp.status_code == 404:
                    write_log('404 error because of proxy', proxy, subcategory_link)
                elif 'Bad Request' in resp.text:
                    write_log('Bad Request', resp.text.strip(), subcategory_link)
                write_log('HTTP error occurred', http_err, subcategory_link)
                continue
            except requests.ConnectionError as conn_err:
                write_log('Connection error occurred', conn_err, subcategory_link)
                continue
            except ConnectionResetError as res_err:
                write_log('ConnectionResetError error occurred', res_err, subcategory_link)
                continue
            except requests.exceptions.ChunkedEncodingError as con_err:
                write_log('ChunkedEncodingError error occurred', con_err, subcategory_link)
                continue
            except requests.exceptions.ReadTimeout as time_err:
                write_log('Time error occurred', time_err, subcategory_link)
                continue
            except KeyError:
                write_log('Bad proxy', resp.headers, subcategory_link)
                continue


# read subcategory links from file
def get_all_product_links():
    subcategory_links = get_csv('subcategory-list-url.csv')
    for subcategory_link in subcategory_links:
        get_product_links(subcategory_link[1])
# End pars all products from subcategories ****************


# Start pars all products data ******************
# get product name, price, details, size, image link
def get_product_data(html, product_link):
    soup = BeautifulSoup(html, "html.parser")
    detail, size, image = [], [], []
    try:
        name = soup.find('h1', id='productName').text
    except AttributeError:
        name = ''
    try:
        price = soup.find('span', id='p_price').find(class_='productBasePrice').getText()
    except AttributeError:
        try:
            price = soup.find('span', id='p_price').find(class_='productSpecialPrice').getText()
        except AttributeError:
            price = ''
    try:
        detail_list = soup.find('ul', id='productDetailsList').find_all('li')
        for d in detail_list:
            detail.append(d.getText())
    except AttributeError:
        detail.append('')
    try:
        size_list = soup.find('ul', id='attrib-1').find_all('span')
        for s in size_list:
            size.append(s.text)
    except AttributeError:
        size.append('')
    try:
        image_links = soup.find('div', id='productsImageWrapper').find_all('li')
        for image_link in image_links:
            image.append('http://www.i28.com/' + image_link.find('a').get('lpic'))
    except AttributeError:
        image.append('')
    data = {'product-link': product_link,
            'product-name': name,
            'product-price': price,
            'product-detail': detail,
            'product-size': size,
            'product-image': image}
    write_csv(data, 'product-link', 'product-name', 'product-price', 'product-detail', 'product-size', 'product-image', 'product-list.csv')


# get product data
def get_all_product_data():
    product_links = get_csv('product_list_url.csv')
    proxies = get_proxies('hideme_proxy.csv')
    sum_proxies = len(list(get_proxies('hideme_proxy.csv')))
    count = 0
    for product_link in product_links:
        while True:
            try:
                if count == sum_proxies - 1:
                    write_log('End of proxy file', 'started file from the beginning', '')
                    del proxies
                    proxies = get_proxies('hideme_proxy.csv')
                    count = 0
                resp, proxy = get_html(product_link[1], proxies)
                if 13000 >= int(resp.headers['Content-Length']):
                    raise KeyError
                resp.raise_for_status()
                if 'Bad Request' in resp.text:
                    raise requests.HTTPError
                get_product_data(resp.text, product_link[1])
                break
            except requests.HTTPError as http_err:
                if resp.status_code == 403:
                    write_log('403 error because of proxy', proxy, product_link[1])
                elif resp.status_code == 404:
                    write_log('404 error because of proxy', proxy, product_link[1])
                elif 'Bad Request' in resp.text:
                    write_log('Bad Request', resp.text.strip(), product_link[1])
                write_log('HTTP error occurred', http_err, product_link[1])
                continue
            except requests.ConnectionError as conn_err:
                write_log('Connection error occurred', conn_err, product_link[1])
                continue
            except ConnectionResetError as res_err:
                write_log('ConnectionResetError error occurred', res_err, product_link[1])
                continue
            except requests.exceptions.ChunkedEncodingError as con_err:
                write_log('ChunkedEncodingError error occurred', con_err, product_link[1])
                continue
            except requests.exceptions.ReadTimeout as time_err:
                write_log('Time error occurred', time_err, product_link[1])
                continue
            except KeyError:
                write_log('Bad proxy', resp.headers, product_link[1])
                continue
# End pars all products data ******************


# read from file
def get_csv(file):
    data = []
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter=';', quotechar='"')
        for row in reader:
            data.append(row)
    return data


# write to file
def write_csv(data, a, b, c, d, e, f, file):
    with open(file, 'a') as name:
        writer = csv.writer(name, delimiter=';')
        writer.writerow((data[a],
                         data[b],
                         data[c],
                         data[d],
                         data[e],
                         data[f]))


# write log to file
def write_log(text, err, link):
    print('Error, look at the "log.csv"')
    with open('log.csv', 'a') as log:
        writer = csv.writer(log, delimiter=';')
        writer.writerow((datetime.now(), text, err, link))


def main():
    start = datetime.now()
    print('Start: ', start)

    get_category_links()

    get_subcategory_links()

    get_all_product_links()

    get_all_product_data()

    end = datetime.now()
    print('Total: ', str(end - start))


if __name__ == '__main__':
    main()
