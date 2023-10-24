import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from time import sleep

logger = logging.getLogger()
logger.setLevel(logging.DEBUG) 

log_folder_path = 'C:\\FTP\\krmart\\GTV\\furni\\фото\\furni\\6\\script_update\\log_furniset'
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
    #sleep(2)
    #response = session.get(url, headers=headers)
    #soup = BeautifulSoup(response.content, 'html.parser')
    
    price_elem = soup.select_one('.cost .price .price_value')
    if price_elem:
        price = float(price_elem.text.replace("грн", "").replace(",", ".").replace(" ", ""))
    else:
        price = 0
    
    quantity_elem = soup.select_one('.counter_block .plus')
    if quantity_elem:
        quantity_text = re.findall(r'\d+', quantity_elem.attrs.get('data-max', '0'))
        quantity = int(quantity_text[0]) if quantity_text else 0
    else:
        quantity = 0
    
    return {f'Цена_{field_name}': price, f'Кол-во_{field_name}': quantity}

def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537'
    }
    print("Reading URLs from file...")
    file_path = 'C:\\FTP\\krmart\\GTV\\furni\\фото\\furni\\6\\script_update\\art_gtv_hogert_ss_test.txt'
    with open(file_path, 'r') as f:
        urls = f.readlines()

    # Создание DataFrame
    df = pd.DataFrame(columns=['№', 'Артикул', 'Наименование', 'код товара', 'мл.категория', 'Ссылка', 'Цена_GTV', 'Цена_FURNISET', 'Цена_KRmart', 'Кол-во_KRmart', 'Кол-во_GTV', 'Кол-во_FURNISET', 'Производитель', 'Рекомендованые'])

    with requests.Session() as session:
        session.headers.update(headers)
        print("Starting main loop for URLs...")
        
        for url in urls:
            try:
                response = session.get(url.strip())
                print(f"Status Code for {url.strip()}: {response.status_code}")
                if response.status_code != 200:
                    logging.warning(f"Не удалось загрузить страницу: {url}, Status Code: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                row_to_write = {}

                sleep(2)
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
                df = df.append(row_to_write, ignore_index=True)  # Добавляем данные в df в блоке try

            except Exception as e:
                print(f"Ошибка при обработке URL {url}: {e}")
                logger.error(f"Error processing URL {url}. Error: {e}")
                print("Final DataFrame:")
                print(df)

        print("Сохранение собранных данных в output_combined.xlsx...")
        df.to_excel("C:\\FTP\\krmart\\GTV\\furni\\фото\\furni\\6\\script_update\\output_combined.xlsx", index=False)
        print("Данные успешно сохранены!")
    
    # Чтение артикулов из файла
    with open('C:\\FTP\\krmart\\GTV\\furni\\фото\\furni\\6\\script_update\\art_gtv_hogert_test.txt', 'r') as f:
        articles = f.readlines()

    # Удаление лишних пробелов и получение уникальных артикулов
    unique_articles = set(article.strip() for article in articles)


    print("Начало обработки уникальных артикулов")
    for art_value in unique_articles:
        try:
            print(f"Обработка артикула: {art_value}")

            # Обработка GTV
            url_gtv = f"https://gtv.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8"
            response_gtv = session.get(url_gtv, headers=headers)
            soup_gtv = BeautifulSoup(response_gtv.content, 'html.parser')
            
            # Получение названия продукта для GTV
            name_elem_gtv = soup_gtv.select_one('.item-title span')
            product_name_gtv = name_elem_gtv.text.strip() if name_elem_gtv else "Не найдено"
            
            # Получение дополнительных данных с gtv.com.ua
            gtv_data = get_additional_data(soup_gtv, 'GTV')
            
            # Обработка REJS
            url_rejs = f"https://rejs.com.ua/catalog/?q={art_value}&s=%D0%97%D0%BD%D0%B0%D0%B9%D1%82%D0%B8"
            response_rejs = session.get(url_rejs, headers=headers)
            soup_rejs = BeautifulSoup(response_rejs.content, 'html.parser')
            
            # Получение дополнительных данных с rejs.com.ua
            rejs_data = get_additional_data(soup_rejs, 'REJS')

            # Обновление DataFrame
            row_to_write = {
                'Артикул': art_value,
                'Наименование': product_name_gtv
            }
            df.loc[len(df)] = {**row_to_write, **gtv_data, **rejs_data}

        except Exception as e:
            print(f"Ошибка при обработке артикула {art_value}: {e}")
            logger.error(f"Error processing article {art_value}. Error: {e}") 

    # Сохранение файла после обработки всех артикулов
    print("Количество строк в DataFrame перед сохранением:", len(df))
    print(df.head())
    output_file_path = 'C:\\FTP\\krmart\\GTV\\furni\\фото\\furni\\6\\script_update\\output_combined.xlsx'
    print("Содержимое DataFrame перед сохранением:")
    print(df)
    df.to_excel(output_file_path, index=False)
    print(f"Файл успешно сохранен по пути: {output_file_path}")
              

    

if __name__ == "__main__":
    main()
