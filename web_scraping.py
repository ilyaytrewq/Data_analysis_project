"""
Часть 2. Веб-скрейпинг: сбор данных о книгах с сайта books.toscrape.com
Авторы: Тихонов Илья, Тахунов Арсен

Описание задачи:
    Собираем данные о книгах с сайта books.toscrape.com — учебного ресурса,
    специально созданного для практики веб-скрейпинга. Сайт содержит каталог
    из 1000 книг, разбитых по жанрам. Для каждой книги доступна отдельная
    веб-страница с подробной информацией: название, жанр, цена, рейтинг,
    наличие на складе, описание и другие характеристики.

    Мы обходим каталог, собираем ссылки на отдельные страницы книг, а затем
    парсим каждую из них, извлекая 13 признаков. Итоговый датасет (≥ 100 строк,
    каждая строка — данные с отдельной веб-страницы) сохраняется в файл Excel.

Используемые техники (из семинаров НИС):
    - requests + fake_useragent (имитация браузера)
    - BeautifulSoup (парсинг HTML)
    - time.sleep + random (задержки между запросами)
    - tqdm (отображение прогресса)
    - pandas + to_excel (формирование и сохранение таблицы)
"""

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import time
import random
import re
from typing import Optional
from tqdm import tqdm

# ============================================================
# Настройки
# ============================================================
BASE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = BASE_URL + "catalogue/"
NUM_CATALOGUE_PAGES = 10          # 10 страниц каталога × 20 книг = 200 книг
OUTPUT_FILE = "Tikhonov_Takhunov_books.xlsx"

# Маппинг текстовых рейтингов в числа (как на сайте)
RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
}


# ============================================================
# Вспомогательные функции
# ============================================================

def get_page(url: str) -> Optional[BeautifulSoup]:
    """Загружаем веб-страницу с помощью requests и fake_useragent."""
    try:
        response = requests.get(url, headers={"User-Agent": UserAgent().chrome}, timeout=15)
        if response.ok:
            return BeautifulSoup(response.text, "html.parser")
        else:
            print(f"  Ошибка {response.status_code} при загрузке {url}")
            return None
    except requests.RequestException as e:
        print(f"  Исключение при загрузке {url}: {e}")
        return None


def polite_sleep():
    """Случайная задержка между запросами, чтобы не перегружать сервер."""
    time.sleep(random.uniform(0.3, 1.0))


# ============================================================
# Шаг 1. Сбор ссылок на книги со страниц каталога
# ============================================================

def collect_book_urls(num_pages: int = NUM_CATALOGUE_PAGES) -> list:
    """Обходим страницы каталога и собираем URL-адреса отдельных книг."""
    book_urls = []

    print(f"Собираем ссылки на книги с {num_pages} страниц каталога...")

    for page_num in tqdm(range(1, num_pages + 1), desc="Каталог"):
        if page_num == 1:
            url = CATALOGUE_URL + "page-1.html"
        else:
            url = CATALOGUE_URL + f"page-{page_num}.html"

        soup = get_page(url)
        if soup is None:
            continue

        # Каждая книга находится внутри <article class="product_pod">
        articles = soup.find_all("article", class_="product_pod")
        for article in articles:
            link_tag = article.find("h3").find("a")
            if link_tag:
                href = link_tag["href"]
                # Приводим относительную ссылку к абсолютной
                full_url = CATALOGUE_URL + href.replace("../", "")
                book_urls.append(full_url)

        polite_sleep()

    print(f"Найдено {len(book_urls)} ссылок на книги.\n")
    return book_urls


# ============================================================
# Шаг 2. Парсинг страницы отдельной книги
# ============================================================

def parse_book_page(url: str) -> Optional[dict]:
    """Извлекаем данные с отдельной страницы книги."""
    soup = get_page(url)
    if soup is None:
        return None

    book = {}

    # --- Название книги ---
    title_tag = soup.find("div", class_="product_main")
    book["title"] = title_tag.find("h1").get_text(strip=True) if title_tag else ""

    # --- Категория (жанр) ---
    breadcrumb = soup.find("ul", class_="breadcrumb")
    if breadcrumb:
        crumbs = breadcrumb.find_all("li")
        # Структура: Home > Books > <Категория> > <Книга>
        book["category"] = crumbs[2].get_text(strip=True) if len(crumbs) > 2 else ""
    else:
        book["category"] = ""

    # --- Рейтинг (1-5 звёзд) ---
    star_tag = soup.find("p", class_="star-rating")
    if star_tag:
        # Класс вида "star-rating Three"
        rating_class = [c for c in star_tag.get("class", []) if c != "star-rating"]
        book["rating"] = RATING_MAP.get(rating_class[0], 0) if rating_class else 0
    else:
        book["rating"] = 0

    # --- Таблица характеристик (Product Information) ---
    table = soup.find("table", class_="table-striped")
    info = {}
    if table:
        for row in table.find_all("tr"):
            header = row.find("th").get_text(strip=True)
            value = row.find("td").get_text(strip=True)
            info[header] = value

    book["upc"] = info.get("UPC", "")
    book["product_type"] = info.get("Product Type", "")

    # Извлекаем числовые значения цен (убираем символ валюты)
    def parse_price(raw: str) -> float:
        """Извлекаем число из строки вида '£51.77'."""
        match = re.search(r"[\d.]+", raw)
        return float(match.group()) if match else 0.0

    book["price_excl_tax"] = parse_price(info.get("Price (excl. tax)", "0"))
    book["price_incl_tax"] = parse_price(info.get("Price (incl. tax)", "0"))
    book["tax"] = parse_price(info.get("Tax", "0"))
    book["price"] = book["price_incl_tax"]  # итоговая цена

    # Наличие: "In stock (22 available)"
    avail_raw = info.get("Availability", "")
    book["availability"] = "In stock" if "In stock" in avail_raw else "Out of stock"
    avail_match = re.search(r"\((\d+) available\)", avail_raw)
    book["num_available"] = int(avail_match.group(1)) if avail_match else 0

    book["num_reviews"] = int(info.get("Number of reviews", 0))

    # --- Описание книги ---
    desc_tag = soup.find("article", class_="product_page")
    if desc_tag:
        p_tag = desc_tag.find("div", id="product_description")
        if p_tag:
            next_p = p_tag.find_next_sibling("p")
            book["description"] = next_p.get_text(strip=True) if next_p else ""
        else:
            book["description"] = ""
    else:
        book["description"] = ""

    # --- URL страницы ---
    book["book_url"] = url

    return book


# ============================================================
# Шаг 3. Основной пайплайн
# ============================================================

def main():
    print("=" * 60)
    print("Часть 2. Веб-скрейпинг: сбор данных о книгах")
    print("Сайт: books.toscrape.com")
    print("=" * 60, "\n")

    # 1) Собираем ссылки на книги
    book_urls = collect_book_urls()

    # 2) Парсим каждую страницу книги
    print(f"Парсим {len(book_urls)} страниц книг...")
    books_data = []

    for url in tqdm(book_urls, desc="Книги"):
        book = parse_book_page(url)
        if book:
            books_data.append(book)
        polite_sleep()

    print(f"\nУспешно собрано данных о {len(books_data)} книгах.\n")

    # 3) Формируем DataFrame
    columns_order = [
        "title",
        "category",
        "rating",
        "price",
        "price_excl_tax",
        "price_incl_tax",
        "tax",
        "availability",
        "num_available",
        "num_reviews",
        "upc",
        "product_type",
        "description",
        "book_url",
    ]
    df = pd.DataFrame(books_data, columns=columns_order)

    # Переименовываем столбцы для наглядности
    df.columns = [
        "Название",
        "Категория",
        "Рейтинг (1-5)",
        "Цена (£)",
        "Цена без налога (£)",
        "Цена с налогом (£)",
        "Налог (£)",
        "Наличие",
        "Кол-во на складе",
        "Кол-во отзывов",
        "UPC",
        "Тип продукта",
        "Описание",
        "URL страницы",
    ]

    print(f"Размер таблицы: {df.shape[0]} строк × {df.shape[1]} столбцов")
    print(f"\nСтолбцы: {list(df.columns)}\n")
    print(df.head())

    # 4) Сохраняем в Excel
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"\nДанные сохранены в файл: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
