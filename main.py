'''
Основной модуль.
Скрипт парсинга сайтов поиска работы. 
Каждый описанный класс отвечает за работу к конкретной площадкой.
При изменении параметров поиска рекомендуется удалить headhunter_vacancies.json

'''
import json
import logging
import os
import re
from time import sleep

from bs4 import BeautifulSoup
from fake_headers import Headers
import requests
from tqdm import tqdm
from schedule import repeat, run_pending, every

from searching_config import MAIN_TAG, EXTRA_TAGS, CITIES, NUMBER_OF_PAGES

class HeadHunter:
    '''
    Статический класс для Web-скраппинга сайта https://hh.ru/
    '''
    headers = Headers().generate()

    @staticmethod
    def _push_info_in_json(new_vacancies_info: list[dict], old_vacancies_info: list[dict]):
        '''Метод десериализации собранной информации о вакансиях в файл headhunter_vacancies.json.

        '''
        vacancies_info = {'new': new_vacancies_info, 'old': old_vacancies_info}
        with open('headhunter_vacancies.json', 'w', encoding='utf-8') as fw:
            json.dump(vacancies_info, fw, ensure_ascii=False, indent=4)
        logging.info('Информация по найденным вакансиям доступна по ссылке '
                     f'{os.path.join(os.getcwd(), "headhunter_vacancies.json")}')

    @staticmethod
    def _pull_info_from_json():
        '''Метод сериализации информации о вакансиях из файла headhunter_vacancies.json.
        
        '''
        old_vacancies_links = []
        old_vacancies_info = []
        if os.path.exists('headhunter_vacancies.json'):
            with open('headhunter_vacancies.json', encoding='utf-8') as fr:
                all_vacancies_info = json.load(fr)
            old_vacancies_info = all_vacancies_info['new'] + all_vacancies_info['old']
            old_vacancies_links = {vacancy['link'] for vacancy in old_vacancies_info}
        return old_vacancies_links, old_vacancies_info

    @staticmethod
    def _create_url(main_tag: str, cities: list[str], page: int=0) -> str:
        '''Метод формирования целевого url по заданным параметрам.

        '''
        cities_for_search = {
            'Москва': 1,
            'Санкт-Петербург': 2
        }
        url = 'https://hh.ru/search/vacancy?order_by=publication_time'
        url += f'&text={main_tag.lower()}'
        for city in cities:
            url += f'&area={cities_for_search[city.title()]}'
        url += f'&page={page}'

        logging.info(f'Ссылка для поиска вакансий на странице {page + 1} '
                     f'сформирована:\n{' ' * 25 + url}')
        return url

    @staticmethod
    def _check_vacancy_on_extra_tags(description: str, extra_tags: list[str]) -> bool:
        '''Метод проверки описания вакансии на наличие в ней ключевых слов из extra_tags.

        '''
        flag = True
        for tag in extra_tags:
            result = re.search(f'{tag}', description, flags=re.I)
            flag = flag and bool(result)
            if not flag:
                break
        logging.info(f'Ключевые слова {extra_tags} в описании вакансии '
                     f'{"присутствуют" if flag else "отсутствуют"}')
        return flag

    @staticmethod
    def _find_info_target_vacancy(link: str, extra_tags: list[str]):
        '''Метод поиска и сохранения информации о конкретной вакансии.

        '''
        logging.info(f'Запуск процедуры парсинга страницы\n{' ' * 25 + link}')
        html = requests.get(link, headers=HeadHunter.headers)
        logging.info(f'Целевая вакансия: код ответа - {html.status_code}')

        bsoup = BeautifulSoup(html.text, features='lxml')
        vacancies_info = {}

        description = bsoup.find('div',
                                 {'data-qa': 'vacancy-description'}).text
        flag = HeadHunter._check_vacancy_on_extra_tags(description, extra_tags)
        if not flag:
            return vacancies_info

        vacancies_info['link'] = link

        position = bsoup.find('h1',
                              {'data-qa': 'vacancy-title',
                               'class': 'bloko-header-section-1'}).text
        vacancies_info['position'] = position.replace('\xa0', ' ')

        try:
            salary = bsoup.find('div',
                                {'data-qa': 'vacancy-salary'}).text
        except AttributeError:
            logging.warning('Уровень дохода вакансии не указан')
            salary = 'не указано'
        vacancies_info['salary'] = salary.replace('\xa0', '.')

        company = bsoup.find('span',
                             {'data-qa': 'bloko-header-2',
                              'class': 'bloko-header-section-2 bloko-header-section-2_lite'}).text
        vacancies_info['company'] = company.replace('\xa0', ' ')

        try:
            address = bsoup.find('span',
                                 {'data-qa': 'vacancy-view-raw-address'}).text
        except AttributeError:
            logging.warning('Адрес компании не указан')
            address = bsoup.find('p',
                                 {'data-qa': 'vacancy-view-location'}).text
        vacancies_info['address'] = address.replace('\xa0', ' ')
        return vacancies_info

    @staticmethod
    def find_vacancies_on_page(main_tag: str, cities: list[str],
                               extra_tags: list[str], page: int,
                               old_vacancies_links: list[str]=None):
        '''Метод поиска вакансий по заданным параметрам на одной странице.

                   :main_tag: целевой поисковый запрос;
                     :cities: интересуемые регионы;
                 :extra_tags: дополнительные параметры поиска;
                       :page: номер страницы, на которой будет осуществлен поиск;
        :old_vacancies_links: список найденных ранее вакансий, используется при поиске 
                              вакансий на нескольких страницах из функции find_all_vacancies

        '''
        new_vacancies_info_on_page = []

        url = HeadHunter._create_url(main_tag, cities, page)
        page_html = requests.get(url, headers=HeadHunter.headers)
        logging.info(f'Список вакансий на странице {page + 1}: код ответа - {page_html.status_code}')

        page_bsoup = BeautifulSoup(page_html.text, features='lxml')
        all_vacancies = page_bsoup.find_all('h2',
                                            {'data-qa': 'bloko-header-2',
                                             'class': 'bloko-header-section-2'})
        logging.info(f"По запросу '{main_tag}' в городах {cities} на странице "
                     f'{page + 1} найдено {len(all_vacancies)} вакансий')

        pbar = tqdm(all_vacancies, colour='red', leave=False, dynamic_ncols=True)
        for vacancy in pbar:
            pbar.set_description('Расшифровка страницы')
            link = vacancy.find('a')['href']
            if link in old_vacancies_links:
                continue
            vacancy_info = HeadHunter._find_info_target_vacancy(link, extra_tags)
            if vacancy_info:
                new_vacancies_info_on_page.append(vacancy_info)
        if not new_vacancies_info_on_page:  
            logging.info(f'Новых вакансий на странице {page + 1} с ключевыми cловами {extra_tags}'
                         ' не найдено')
        return new_vacancies_info_on_page

    @repeat(every().day.at('17:00'))
    @staticmethod
    def find_all_vacancies(main_tag: str=MAIN_TAG,
                           cities: list[str]=CITIES,
                           extra_tags: list[str]=EXTRA_TAGS,
                           number_of_pages: int=NUMBER_OF_PAGES):
        '''Метод поиска вакансий по заданным параметрам на нескольких страницах.

               :main_tag: целевой поисковый запрос;
                 :cities: интересуемые регионы;
             :extra_tags: дополнительные параметры поиска;
        :number_of_pages: количество страниц, по которым будет осуществляться поиск.

        По умолчанию параметры ссылаются на аргументы, указанные в searching_config.py.

        '''
        old_vacancies_links, old_vacancies_info = HeadHunter._pull_info_from_json()
        new_vacancies_info = []

        pbar = tqdm(range(number_of_pages), colour='red', dynamic_ncols=True)
        for page in pbar:
            pbar.set_description(f'Поиск вакансий на странице {page + 1}')
            result = HeadHunter.find_vacancies_on_page(main_tag, cities,
                                                       extra_tags, page,
                                                       old_vacancies_links)
            if result:
                new_vacancies_info.extend(result)
        if not new_vacancies_info:
            logging.info(f'Новых вакансий в регионе {cities} с ключевыми '
                         f'cловами {extra_tags} не найдено')
        HeadHunter._push_info_in_json(new_vacancies_info, old_vacancies_info)


def init_logging():
    '''Функция настройки модуля логгирования

    '''
    logging.basicConfig(filename=r'progress.log',
                        filemode='w',
                        encoding='utf-8',
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d-%m-%Y %H:%M:%S')

init_logging()
while True:
    run_pending()
    sleep(1)