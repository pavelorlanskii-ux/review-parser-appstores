import csv
import re
import hashlib
from typing import Optional, Any, List, Dict

import requests
import pandas as pd
import streamlit as st
from langdetect import detect as lang_detect
from langdetect.lang_detect_exception import LangDetectException
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

COLUMNS_ORDER = [
    "review_id","date","rating","title","text","author","country","language","link"
]

def extract_app_id(app_url: str) -> str:
    m = re.search(r"id(\d+)", app_url)
    if not m:
        raise ValueError("В ссылке не найдено id<число>.")
    return m.group(1)

def make_app_page_link(country: str, app_id: str) -> str:
    return f"https://apps.apple.com/{country}/app/id{app_id}"

def safe_detect_language(text: str) -> str:
    text = (text or "").strip()
    if len(text) < 10:
        return "unknown"
    try:
        return lang_detect(text)
    except LangDetectException:
        return "unknown"

def stable_hash_id(country: str, author: str, date: str, rating: str, title: str, text: str) -> str:
    raw = f"{country}|{author}|{date}|{rating}|{title}|{text}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def iso8601(dt_value: Any) -> str:
    ts = pd.to_datetime(dt_value, utc=True, errors="coerce")
    if pd.isna(ts):
        return str(dt_value)
    return ts.isoformat()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=12),
    retry=retry_if_exception_type((requests.RequestException,)),
    reraise=True,
)
def http_get_json(session: requests.Session, url: str, timeout: int = 20) -> dict:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def fetch_reviews_rss(session: requests.Session, app_id: str, country: str,
                      max_per_country: Optional[int], max_pages: int = 10) -> List[Dict]:
    reviews: List[Dict] = []
    processed = 0

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortBy=mostRecent/json"
        else:
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json"

        data = http_get_json(session, url)
        entries = (data.get("feed", {}) or {}).get("entry", [])

        if not entries or not isinstance(entries, list) or len(entries) <= 1:
            break

        for entry in entries[1:]:
            processed += 1
            if max_per_country is not None and processed > max_per_country:
                return reviews

            review_id = ((entry.get("id", {}) or {}).get("label", "") or "").strip()
            link = (((entry.get("link", {}) or {}).get("attributes", {}) or {}).get("href", "") or "").strip()
            title = (entry.get("title", {}) or {}).get("label", "") or ""
            text = (entry.get("content", {}) or {}).get("label", "") or ""
            author = ((entry.get("author", {}) or {}).get("name", {}) or {}).get("label", "") or ""
            rating = (entry.get("im:rating", {}) or {}).get("label", "") or ""
            date_val = (entry.get("updated", {}) or {}).get("label", "") or ""
            date_str = iso8601(date_val)

            if not review_id:
                review_id = stable_hash_id(country, author, date_str, str(rating), title, text)
            if not link:
                link = make_app_page_link(country, app_id)

            language = safe_detect_language(f"{title}\n{text}")

            reviews.append({
                "review_id": str(review_id),
                "date": date_str,
                "rating": int(rating) if str(rating).isdigit() else "",
                "title": title,
                "text": text,
                "author": author,
                "country": country,
                "language": language,
                "link": link,
            })

    return reviews

def update_reviews(app_url: str, country_list: List[str], max_per_country: Optional[int] = None) -> pd.DataFrame:
    app_id = extract_app_id(app_url)
    csv_path = f"appstore_reviews_{app_id}.csv"

    try:
        existing_df = pd.read_csv(csv_path, dtype={"review_id": str}, keep_default_na=False)
        for col in COLUMNS_ORDER:
            if col not in existing_df.columns:
                existing_df[col] = ""
        existing_df = existing_df[COLUMNS_ORDER]
        existing_ids = set(existing_df["review_id"].astype(str))
    except FileNotFoundError:
        existing_df = pd.DataFrame(columns=COLUMNS_ORDER)
        existing_ids = set()

    new_rows = []
    new_ids = set()

    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0 (reviews-collector/1.0)"})
        for country in country_list:
            reviews = fetch_reviews_rss(session, app_id, country, max_per_country=max_per_country, max_pages=10)
            for r in reviews:
                rid = r["review_id"]
                if rid in existing_ids or rid in new_ids:
                    continue
                new_rows.append({c: r.get(c, "") for c in COLUMNS_ORDER})
                new_ids.add(rid)

    if new_rows:
        combined = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
    else:
        combined = existing_df.copy()

    combined = combined.drop_duplicates(subset=["review_id"], keep="first")
    combined.to_csv(
        csv_path,
        index=False,
        encoding="utf-8",
        sep=",",
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        lineterminator="\n",
    )
    return combined


# ---------- UI ----------
st.set_page_config(page_title="App Store Reviews", layout="wide")
st.title("Узнать свои отзывы в App Store")

st.caption("Собираем отзывы только из App Store.")

app_url = st.text_input("Ссылка на приложение", value="")
countries = st.text_input("Введите страны", value="")
st.caption("Если нужно несколько стран, вводите через запятую. Например: ru, us, de")
max_n = st.number_input("Сколько отзывов собрать из каждой страны", min_value=1, value=200, step=1)

country_list = [c.strip().lower() for c in countries.split(",") if c.strip()]
max_per_country = int(max_n)

if st.button("Собрать отзывы"):
    df = update_reviews(app_url, country_list, max_per_country=max_per_country)
    st.success(f"Готово! Всего отзывов: {len(df)}")
    st.dataframe(df.tail(20), use_container_width=True)

    app_id = extract_app_id(app_url)
    csv_path = f"appstore_reviews_{app_id}.csv"
    with open(csv_path, "rb") as f:
        st.download_button("Скачать CSV", f, file_name=csv_path, mime="text/csv")
