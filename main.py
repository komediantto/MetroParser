import datetime
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

options = Options()
options.add_argument('-headless')


HOME_URL = 'https://online.metro-cc.ru'
URL = 'https://online.metro-cc.ru/category/chaj-kofe-kakao/chay?page='


def get_data() -> list:
    card_html = []
    driver = webdriver.Firefox(options=options)

    for i in tqdm(range(1, 9), desc='Searching...'):
        try:
            driver.get(URL + str(i))
        except Exception as e:
            print(f'Ошибка сервера, {e}')
        card_list = driver.find_elements(
            By.CLASS_NAME, "product-card__content")
        card_html += [card.get_attribute('outerHTML') for card in card_list]

    driver.quit()

    return card_html


def parse_html(html_list: list) -> List[dict]:
    result_list = []

    for html in tqdm(html_list, desc='Collect data...'):
        soup = BeautifulSoup(html, 'lxml')
        prices = soup.find_all('span', class_='product-price__sum-rubles')

        if len(prices) == 1:
            regular_price = prices[0].text
            promo_price = None
        elif len(prices) == 2:
            regular_price = prices[1].text
            promo_price = prices[0].text
        else:
            continue

        name = soup.find('span', class_='product-card-name__text').text
        link = soup.find('a', class_='product-card-name reset-link catalog-2-level-product-card__name style--catalog-2-level-product-card').get('href')

        result = {
            'id': None,
            'name': name.strip().replace('\n', '').strip(),
            'link': HOME_URL + link,
            'regular-price': regular_price,
            'promo-price': promo_price,
            'brand': None
        }

        result_list.append(result)

    return result_list


def get_articul_and_brand(url):
    driver = webdriver.Firefox(options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(url)
    articul_locator = (By.CSS_SELECTOR, '[itemprop="productID"]')
    brand_locator = (By.CLASS_NAME, 'product-attributes__list-item-link')
    articul = wait.until(
        EC.visibility_of_element_located(articul_locator)).text
    brand = wait.until(EC.visibility_of_element_located(brand_locator)).text

    driver.quit()

    return articul, brand


def create_full_dict_list(items):
    futures = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i, item in enumerate(items):
            future = executor.submit(get_articul_and_brand, item['link'])
            futures.append(future)
            future.item_index = i

    wait(futures)

    for future in futures:
        articul, brand = future.result()
        item_index = future.item_index
        items[item_index]['id'] = articul
        items[item_index]['brand'] = brand

    return items


def create_xlsx(dict_list):
    df = pd.DataFrame(dict_list)
    date = datetime.datetime.now().strftime('%H:%M_%d-%m-%Y')
    df.to_excel(f'{date}.xlsx', index=False)
    print(f'Файл {date}.xlsx создан')


if __name__ == '__main__':
    data = get_data()
    items = parse_html(data)
    print('Check availability...')
    list_of_dicts = create_full_dict_list(items)
    create_xlsx(list_of_dicts)
