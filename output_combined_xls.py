import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from time import sleep

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'
    }

logger = logging.getLogger()
logger.setLevel(logging.DEBUG) 

log_folder_path = '/home/kuhnisol/krmart.in.ua/bot/script_update/log_furniset'
log_file_name = f"{log_folder_path}/scraper_{datetime.now().strftime('%Y%m%d_%H%M%S_furniset')}.log"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG) 

file_handler_all = logging.FileHandler(log_file_name)
file_handler_all.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler_all.setFormatter(formatter)

logger.addHandler(file_handler_all)

logger.info("This is an info message for furniset script.")
logger.error("This is an error message for furniset script.")
start_time = datetime.now()

def get_additional_data(soup, field_name):
    # Для цены
    price_elem = soup.select_one('.price .price_value')
    if price_elem:
        price = float(price_elem.text.replace("грн", "").replace(" ", "").strip())
    else:
        price = 0
    
    # Для количества
    quantity_elem = soup.select_one('.plus')
    if quantity_elem:
        quantity = int(quantity_elem.get('data-max', 0))
    else:
        quantity = 0

    
    return {f'Цена_{field_name}': price, f'Кол-во_{field_name}': quantity}

def main():
    file_path = '/home/kuhnisol/krmart.in.ua/bot/script_update/art_gtv_hogert_ss.txt'
    with open(file_path, 'r') as f:
        urls = f.readlines()

    # Создание DataFrame
    df = pd.DataFrame(columns=['№', 'Артикул', 'Наименование', 'код товара', 'мл.категория', 'Ссылка', 'Цена_GTV', 'Цена_FURNISET', 'Цена_KRmart', 'Кол-во_KRmart', 'Кол-во_GTV', 'Кол-во_FURNISET', 'Производитель', 'Рекомендованые'])

    with requests.Session() as session:
        session.headers.update(headers)

        session_gtv = requests.Session()
        session_rejs = requests.Session()
        
        for url in urls:
            try:
                response = session.get(url.strip())
                print(f"Status Code for {url.strip()}: {response.status_code}")
                if response.status_code != 200:
                    logging.warning(f"Не удалось загрузить страницу: {url}, Status Code: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                row_to_write = {}

                sleep(5)
                price_element = soup.select_one("span.tov_cena")
                price = price_element.text.strip() if price_element else "не указана"
                print(f"Price: {price}")
                row_to_write['Цена_FURNISET'] = price
                          
                name_h1_ru = soup.find('h1')
                name_ru = name_h1_ru.get_text() if name_h1_ru else "Не найдено"
                row_to_write['Наименование'] = name_ru
          
                details_div = soup.find('div', class_='tov_info')
                article = details_div.get_text().split('Код: ')[1].strip() if details_div else "Не найдено"
                row_to_write['код товара'] = article

            
                art_div = soup.find('div', class_='tov_info', string=re.compile('Арт.:'))
                if art_div:
                    art_value = art_div.get_text().split('Арт.: ')[1].strip()
                else:
                    art_value = "Не найдено"

                row_to_write['Артикул'] = art_value
            
                manufacturer_div = soup.find('div', class_='tov_info', string=re.compile('Производитель:'))
                if manufacturer_div:
                    manufacturer = manufacturer_div.get_text().split('Производитель: ')[1].strip()
                else:
                    manufacturer = "Не указано"

                row_to_write['Производитель'] = manufacturer
                
            
                availability_divs = soup.find_all('div', style=lambda value: value and "margin-top: 10px;" in value)
                availability_text = next((div.get_text().strip() for div in availability_divs), "Статус наличия неизвестен")

                availability_map = {
                    "Ожидается поставка, сроки уточняйте": "0",
                    "Товар в наличии": "100",
                    "Товар заканчивается, уточняйте наличие": "5",
                }
                row_to_write['Кол-во_FURNISET'] = availability_map.get(availability_text, "0")
                row_to_write['Ссылка'] = f'{url}'
                print(f"Row to Write: {row_to_write}")
                # Получение данных с gtv.com.ua
                response_gtv = session.get(f"https://gtv.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8", headers=headers)
                sleep(5) 
                soup_gtv = BeautifulSoup(response_gtv.content, 'html.parser')
                gtv_data = get_additional_data(soup_gtv, 'GTV')
                if gtv_data:
                    row_to_write['Цена_GTV'] = gtv_data.get('Цена_GTV', 0)  
                    row_to_write['Кол-во_GTV'] = gtv_data.get('Кол-во_GTV', 0) 

                # Получение данных с rejs.com.ua
                response_rejs = session.get(f"https://rejs.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8", headers=headers)
                sleep(5) 
                soup_rejs = BeautifulSoup(response_rejs.content, 'html.parser')
                rejs_data = get_additional_data(soup_rejs, 'REJS')
                if rejs_data:
                    row_to_write['Цена_GTV'] += rejs_data.get('Цена_REJS', 0) 
                    row_to_write['Кол-во_GTV'] += rejs_data.get('Кол-во_REJS', 0)

                df = pd.concat([df, pd.DataFrame([row_to_write])], ignore_index=True)
                
                
                
            except Exception as e:
                print(f"Ошибка при обработке URL {url}: {e}")
                logger.error(f"Error processing URL {url}. Error: {e}")
                print("Final DataFrame:")
                

        print("Сохранение собранных данных в output_combined.xlsx...")
        df.to_excel("/home/kuhnisol/krmart.in.ua/bot/script_update/furniset/output_combined.xlsx", index=False)
        print("Данные успешно сохранены!")
    
    
        # Считывание артикулов из файла
    with open('/home/kuhnisol/krmart.in.ua/bot/script_update/art_gtv_hogert_test.txt', 'r') as f:
        articles_from_file = [line.strip() for line in f.readlines()]

    # Получение артикулов из DataFrame
    articles_from_df = df['Артикул'].tolist()

    # Объединение двух списков и удаление дубликатов
    all_articles = list(set(articles_from_file + articles_from_df))

    # Удаление лишних пробелов и получение уникальных артикулов
    unique_articles = set(article.strip() for article in all_articles)


    
    for art_value in all_articles:
        sleep(5) 
        try:
            if art_value in df['Артикул'].values:
                continue

            # Обработка GTV
            url_gtv = f"https://gtv.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8"
            response_gtv = session_gtv.get(url_gtv, headers=headers)
            sleep(5)
            soup_gtv = BeautifulSoup(response_gtv.content, 'html.parser')
            
            # Получение названия продукта для GTV
            name_elem_gtv = soup_gtv.select_one('.item-title span')
            product_name_gtv = name_elem_gtv.text.strip() if name_elem_gtv else "Не найдено"
            
            # Получение дополнительных данных с gtv.com.ua
            gtv_data = get_additional_data(soup_gtv, 'GTV')
            
            # Обработка REJS
            url_rejs = f"https://rejs.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8"
            response_rejs = session_rejs.get(url_rejs, headers=headers)
            sleep(5)
            soup_rejs = BeautifulSoup(response_rejs.content, 'html.parser')
            
            # Получение дополнительных данных с rejs.com.ua
            rejs_data = get_additional_data(soup_rejs, 'REJS')

            # Обновление DataFrame
            row_to_write = {
                'Артикул': art_value,
                'Наименование': product_name_gtv
            }
            row_to_write.update(gtv_data)
            row_to_write.update(rejs_data)
            df.loc[len(df)] = row_to_write
            
            # Добавьте задержку перед следующим запросом
            sleep(10)

        except Exception as e:
            print(f"Ошибка при обработке артикула {art_value}: {e}")
            logger.error(f"Error processing article {art_value}. Error: {e}") 

    # Сохранение файла после обработки всех артикулов
    output_file_path = '/home/kuhnisol/krmart.in.ua/bot/script_update/furniset/output_combined.xlsx'
    df.to_excel(output_file_path, index=False)
    
              

    

if __name__ == "__main__":
    main()
