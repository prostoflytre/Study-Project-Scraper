import requests
import json
import pandas as pd
from retry import retry

def fetch_full_catalog() -> dict:
    # Получаем каталог Wildberries.
    catalog_url = 'https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json'
    headers = {'Accept': '*/*', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    return requests.get(catalog_url, headers=headers).json()

def extract_category_data(catalog_data: dict) -> list:
    # Извлечение данных категорий из каталога Wildberries.
    category_list = []
    if isinstance(catalog_data, dict) and 'childs' not in catalog_data:
        category_list.append({
            'category_name': catalog_data['name'],
            'shard_key': catalog_data.get('shard', None),
            'category_url': catalog_data['url'],
            'query_params': catalog_data.get('query', None)
        })
    elif isinstance(catalog_data, dict):
        category_list.append({
            'category_name': catalog_data['name'],
            'shard_key': catalog_data.get('shard', None),
            'category_url': catalog_data['url'],
            'query_params': catalog_data.get('query', None)
        })
        category_list.extend(extract_category_data(catalog_data['childs']))
    else:
        for child in catalog_data:
            category_list.extend(extract_category_data(child))
    return category_list

def find_category_by_url(input_url: str, catalog: list) -> dict:
    # Проверка наличия пользовательской ссылки в каталоге.
    for category in catalog:
        if category['category_url'] == input_url.split('https://www.wildberries.ru')[-1]:
            print(f'Совпадение найдено: {category["category_name"]}')
            return category

def parse_json_data(json_response: dict) -> list:
    # Извлечение данных из JSON.
    product_list = []
    for product in json_response['data']['products']:
        product_list.append({
            'product_id': product.get('id'),
            'product_name': product.get('name'),
            'original_price': int(product.get("priceU") / 100),
            'discounted_price': int(product.get('salePriceU') / 100),
            'cashback_points': product.get('feedbackPoints'),
            'discount_percentage': product.get('sale'),
            'brand_name': product.get('brand'),
            'rating_score': product.get('rating'),
            'supplier_info': product.get('supplier'),
            'supplier_rating': product.get('supplierRating'),
            'feedback_count': product.get('feedbacks'),
            'review_rating': product.get('reviewRating'),
            'promo_text_card': product.get('promoTextCard'),
            'promo_text_category': product.get('promoTextCat'),
            'product_link': f'https://www.wildberries.ru/catalog/{product.get("id")}/detail.aspx?targetUrl=BP'
        })
    return product_list

@retry(Exception, tries=-1, delay=0)
def scrape_page_data(page_num: int, shard_key: str, query_params: str, min_price: int, max_price: int, discount_threshold: int = None) -> dict:
    # Сбор данных с отдельной страницы.
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0)"}
    api_url = f'https://catalog.wb.ru/catalog/{shard_key}/catalog?appType=1&curr=rub' \
              f'&dest=-1257786&locale=ru&page={page_num}&priceU={min_price * 100};{max_price * 100}' \
              f'&sort=popular&spp=0&{query_params}&discount={discount_threshold}'
    response = requests.get(api_url, headers=headers)
    print(f'Статус: {response.status_code} Страница {page_num} Сбор данных...')
    return response.json()

def save_to_excel(data: list, file_name: str):
    # Сохранение результата в Excel файл.
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(f'{file_name}.xlsx')
    df.to_excel(writer, sheet_name='products', index=False)

    writer.sheets['products'].set_column(0, 1, width=10)
    writer.sheets['products'].set_column(1, 2, width=34)
    writer.sheets['products'].set_column(2, 3, width=8)
    writer.sheets['products'].set_column(3, 4, width=9)
    writer.sheets['products'].set_column(4, 5, width=8)
    writer.sheets['products'].set_column(5, 6, width=4)
    writer.sheets['products'].set_column(6, 7, width=20)
    writer.sheets['products'].set_column(7, 8, width=6)
    writer.sheets['products'].set_column(8, 9, width=23)
    writer.sheets['products'].set_column(9, 10, width=13)
    writer.sheets['products'].set_column(10, 11, width=11)
    writer.sheets['products'].set_column(11, 12, width=12)
    writer.sheets['products'].set_column(12, 13, width=15)
    writer.sheets['products'].set_column(13, 14, width=15)
    writer.sheets['products'].set_column(14, 15, width=67)
    writer.close()
    print(f'Результат сохранен в {file_name}.xlsx\n')

def main_parser(input_url: str, min_price: int = 1, max_price: int = 1000000, discount_threshold: int = 0):
    # Основная функция парсинга.
    catalog = extract_category_data(fetch_full_catalog())
    try:
        category_data = find_category_by_url(input_url=input_url, catalog=catalog)
        collected_data = []
        for page in range(1, 51):
            page_data = scrape_page_data(
                page_num=page,
                shard_key=category_data['shard_key'],
                query_params=category_data['query_params'],
                min_price=min_price,
                max_price=max_price,
                discount_threshold=discount_threshold)
            parsed_products = parse_json_data(page_data)
            print(f'Добавлено позиций: {len(parsed_products)}')
            if len(parsed_products) > 0:
                collected_data.extend(parsed_products)
            else:
                break
        print(f'Сбор данных завершен. Собрано: {len(collected_data)} товаров.')
        save_to_excel(collected_data, f'{category_data["category_name"]}_from_{min_price}_to_{max_price}')
        print(f'Ссылка для проверки: {input_url}?priceU={min_price * 100};{max_price * 100}&discount={discount_threshold}')
    except TypeError:
        print('Ошибка! Возможно, неверно указан раздел. Убедитесь, что ссылка указана без фильтров.')
    except PermissionError:
        print('Ошибка! Закройте ранее открытый Excel файл и повторите попытку.')

if __name__ == '__main__':
    while True:
        try:
            input_url = input('Введите ссылку на категорию без фильтров для сбора (или "q" для выхода):\n')
            if input_url == 'q':
                break
            min_price = int(input('Введите минимальную цену товара: '))
            max_price = int(input('Введите максимальную цену товара: '))
            discount_threshold = int(input('Введите минимальную скидку (0, если без скидки): '))
            main_parser(input_url=input_url, min_price=min_price, max_price=max_price, discount_threshold=discount_threshold)
        except:
            print('Ошибка ввода данных. Проверьте правильность введенных значений. Перезапуск...')
