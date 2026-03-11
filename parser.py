import asyncio
import aiohttp
import aiofiles
import re
import os
import time
import json
import subprocess
import tempfile
import requests
import threading
import socket
import random
import urllib.parse
import ssl
import sys
import io
import base64
import platform
import signal
from urllib3.exceptions import InsecureRequestWarning
from notworkers_db import NotworkersDB

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# ========= КОДИРОВКА UTF-8 ДЛЯ WINDOWS =========
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import argparse
import configparser
from typing import Optional
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= ДИАГНОСТИКА =========
print(f"🚀 Запуск парсера...")
print(f"📂 Текущая директория: {os.getcwd()}")
print(f"🐍 Python версия: {sys.version}")

# ========= КОНФИГУРАЦИЯ =========
def load_config() -> configparser.ConfigParser:
    """Загружает настройки из config.ini"""
    config = configparser.ConfigParser()
    config_file = Path('config.ini')
    if config_file.exists():
        config.read(config_file, encoding='utf-8')
        print(f"⚙️ Загружена конфигурация из config.ini")
    else:
        print(f"⚠️ Файл config.ini не найден, используются значения по умолчанию")
    return config

_config = load_config()

# ========= ФАЙЛЫ =========
SOURCES_FILE = "sources.txt"
OUTPUT_FILE = "url.txt"
CLEAN_FILE = "url_clean.txt"
FILTERED_FILE = "url_filtered.txt"
NAMED_FILE = "url_named.txt"
ENCODED_FILE = "url_encoded.txt"
WORK_FILE = "url_work.txt"
SPEED_FILE = "url_work_speed.txt"
COMBINED_FILE = "url_combined.txt"
LOG_FILE = "log.txt"
PROCESSED_FILE = "processed.json"
CACHE_FILE = "cache_results.json"
DEBUG_FILE = "debug_failed.txt"
XRAY_LOG_FILE = "xray_errors.log"
NOTWORKERS_FILE = "configs/notworkers.json"
NOTWORKERS_DB = "configs/notworkers.db"

# ========= НАСТРОЙКИ =========
THREADS_DOWNLOAD = _config.getint('threads', 'threads_download', fallback=50)
CYCLE_DELAY = 3600
LOG_CLEAN_INTERVAL = 86400
CYCLES_BEFORE_DEBUG_CLEAN = 5  # Очистка debug_failed.txt каждые 5 циклов

XRAY_MAX_WORKERS = _config.getint('threads', 'xray_max_workers', fallback=30)
XRAY_TEST_URL = _config.get('speed', 'speed_test_url', fallback='https://www.gstatic.com/generate_204')
XRAY_TIMEOUT = _config.getint('timeouts', 'xray_timeout', fallback=8)
MAX_RETRIES = _config.getint('retry', 'max_retries', fallback=3)
RETRY_DELAY_BASE = _config.getfloat('retry', 'retry_delay_base', fallback=1.0)
RETRY_DELAY_MULTIPLIER = _config.getfloat('retry', 'retry_delay_multiplier', fallback=2.0)
TOP_FAST_COUNT = _config.getint('speed', 'top_fast_count', fallback=30)
SPEED_TEST_REQUESTS = _config.getint('speed', 'speed_test_requests', fallback=3)
STRONG_STYLE_ATTEMPTS = _config.getint('speed', 'strong_style_attempts', fallback=3)
STRONG_STYLE_TIMEOUT = _config.getint('speed', 'strong_style_timeout', fallback=12)
STRONG_MAX_RESPONSE_TIME = _config.getint('speed', 'strong_max_response_time', fallback=3)
STRONG_CONNECT_TIMEOUT_RATIO = 0.4
STRONG_CONNECT_TIMEOUT_MIN = 3
STRONG_CONNECT_TIMEOUT_MAX = 10
STRONG_READ_TIMEOUT_MIN = 5
STRONG_ATTEMPT_DELAY = _config.getfloat('speed', 'strong_attempt_delay', fallback=0.15)
# Adaptive timeout: если первый запрос медленнее этого порога, таймаут для следующих увеличивается
ADAPTIVE_TIMEOUT_THRESHOLD_SECONDS = 5.0
ADAPTIVE_TIMEOUT_MULTIPLIER = 1.5
# В xraycheck для generate_204 допускается только очень маленький ответ
GENERATE_204_MAX_CONTENT_LENGTH = 64

NOTWORKERS_ENABLED = _config.getboolean('notworkers', 'notworkers_enabled', fallback=True)
NOTWORKERS_TTL_DAYS = _config.getint('notworkers', 'notworkers_ttl_days', fallback=7)
NOTWORKERS_MIN_FAILS = _config.getint('notworkers', 'notworkers_min_fails', fallback=2)

# Проверка стабильности
STABILITY_CHECKS = _config.getint('stability', 'stability_checks', fallback=2)
STABILITY_DELAY = _config.getfloat('stability', 'stability_delay', fallback=1.0)

# Геолокация
CHECK_GEOLOCATION = _config.getboolean('geolocation', 'check_geolocation', fallback=False)
_allowed_countries_raw = _config.get('geolocation', 'allowed_countries', fallback='')
ALLOWED_COUNTRIES = [c.strip().upper() for c in _allowed_countries_raw.split(',') if c.strip()]
GEOLOCATION_TIMEOUT = _config.getint('geolocation', 'geolocation_timeout', fallback=5)

# Строгий режим (включён по умолчанию для всех протоколов)
STRICT_MODE = _config.getboolean('strict', 'strict_mode', fallback=True)
STRICT_ATTEMPTS = _config.getint('strict', 'strict_attempts', fallback=3)
STRICT_MAX_RESPONSE_TIME = _config.getfloat('strict', 'strict_max_response_time', fallback=3.0)
STRICT_TIMEOUT = _config.getint('strict', 'strict_timeout', fallback=12)
# Минимум успешных запросов из серии (STRONG_STYLE_TEST: 2 из 3)
MIN_SUCCESSFUL_REQUESTS = _config.getint('strict', 'min_successful_requests', fallback=2)

# Проверка HTTPS через прокси
REQUIRE_HTTPS = _config.getboolean('checker', 'require_https', fallback=True)

# Предфильтр (TCP/TLS pre-filter)
PREFILTER_ENABLED = _config.getboolean('prefilter', 'prefilter_enabled', fallback=True)
PREFILTER_WORKERS = _config.getint('prefilter', 'prefilter_workers', fallback=200)
PREFILTER_TCP_TIMEOUT = _config.getint('prefilter', 'prefilter_tcp_timeout', fallback=2)
PREFILTER_TLS_TIMEOUT = _config.getint('prefilter', 'prefilter_tls_timeout', fallback=3)

# Таймаут ожидания запуска Xray (с поллингом порта вместо фиксированного sleep)
XRAY_START_WAIT = _config.getfloat('timeouts', 'xray_start_wait', fallback=2.0)

print(f"⚡ Настройки: XRAY_MAX_WORKERS={XRAY_MAX_WORKERS}, TIMEOUT={XRAY_TIMEOUT}, STRICT_MODE={STRICT_MODE}, MIN_SUCCESSFUL={MIN_SUCCESSFUL_REQUESTS}")

# ========= СЧЕТЧИК ЦИКЛОВ =========
cycle_counter = 0

# ========= РЕГУЛЯРКИ =========
VLESS_REGEX = re.compile(r"vless://[^\s]+", re.IGNORECASE)
PROTOCOL_REGEXES = {
    'vless':     re.compile(r"vless://[^\s]+", re.IGNORECASE),
    'vmess':     re.compile(r"vmess://[^\s]+", re.IGNORECASE),
    'trojan':    re.compile(r"trojan://[^\s]+", re.IGNORECASE),
    'ss':        re.compile(r"(?<![a-zA-Z0-9])ss://[^\s]+", re.IGNORECASE),
    'hysteria2': re.compile(r"(?:hysteria2|hy2)://[^\s]+", re.IGNORECASE),
}
UUID_REGEX = re.compile(
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

# ========= ВАШ ОГРОМНЫЙ СПИСОК ДОМЕНОВ =========
DOMAIN_NAMES = {
    # X5 Retail Group (Пятёрочка, Перекрёсток)
    'x5.ru': 'Пятёрочка',
    '5ka.ru': 'Пятёрочка',
    '5ka-cdn.ru': 'Пятёрочка',
    '5ka.static.ru': 'Пятёрочка',
    'ads.x5.ru': 'Пятёрочка',
    'perekrestok.ru': 'Перекрёсток',
    'vprok.ru': 'Перекрёсток',
    'dixy.ru': 'Дикси',
    
    # Fasssst и его поддомены
    'fasssst.ru': 'Fasssst',
    'rontgen.fasssst.ru': 'Fasssst',
    'res.fasssst.ru': 'Fasssst',
    'yt.fasssst.ru': 'Fasssst',
    'fast.strelkavpn.ru': 'StrelkaVPN',
    'strelkavpn.ru': 'StrelkaVPN',
    
    # Maviks - торговая компания
    'maviks.ru': 'Maviks',
    'ru.maviks.ru': 'Maviks',
    'a.ru.maviks.ru': 'Maviks',
    
    # Tree-top
    'tree-top.cc': 'TreeTop',
    'a.ru.tree-top.cc': 'TreeTop',
    
    # Connect-Iskra
    'connect-iskra.ru': 'Iskra',
    '212-wl.connect-iskra.ru': 'Iskra',
    
    # VK и связанные сервисы
    'vk.com': 'VK',
    'vk.ru': 'VK',
    'vkontakte.ru': 'VK',
    'userapi.com': 'VK',
    'cdn.vk.com': 'VK',
    'cdn.vk.ru': 'VK',
    'id.vk.com': 'VK',
    'id.vk.ru': 'VK',
    'login.vk.com': 'VK',
    'login.vk.ru': 'VK',
    'api.vk.com': 'VK',
    'api.vk.ru': 'VK',
    'im.vk.com': 'VK',
    'm.vk.com': 'VK',
    'm.vk.ru': 'VK',
    'sun6-22.userapi.com': 'VK',
    'sun6-21.userapi.com': 'VK',
    'sun6-20.userapi.com': 'VK',
    'sun9-38.userapi.com': 'VK',
    'sun9-101.userapi.com': 'VK',
    'pptest.userapi.com': 'VK',
    'vk-portal.net': 'VK',
    'stats.vk-portal.net': 'VK',
    'akashi.vk-portal.net': 'VK',
    'vkvideo.ru': 'VK Видео',
    'm.vkvideo.ru': 'VK Видео',
    'queuev4.vk.com': 'VK',
    'eh.vk.com': 'VK',
    'cloud.vk.com': 'VK',
    'cloud.vk.ru': 'VK',
    'admin.cs7777.vk.ru': 'VK',
    'admin.tau.vk.ru': 'VK',
    'analytics.vk.ru': 'VK',
    'api.cs7777.vk.ru': 'VK',
    'api.tau.vk.ru': 'VK',
    'away.cs7777.vk.ru': 'VK',
    'away.tau.vk.ru': 'VK',
    'business.vk.ru': 'VK',
    'connect.cs7777.vk.ru': 'VK',
    'cs7777.vk.ru': 'VK',
    'dev.cs7777.vk.ru': 'VK',
    'dev.tau.vk.ru': 'VK',
    'expert.vk.ru': 'VK',
    'id.cs7777.vk.ru': 'VK',
    'id.tau.vk.ru': 'VK',
    'login.cs7777.vk.ru': 'VK',
    'login.tau.vk.ru': 'VK',
    'm.cs7777.vk.ru': 'VK',
    'm.tau.vk.ru': 'VK',
    'm.vkvideo.cs7777.vk.ru': 'VK Видео',
    'me.cs7777.vk.ru': 'VK',
    'ms.cs7777.vk.ru': 'VK',
    'music.vk.ru': 'VK Музыка',
    'oauth.cs7777.vk.ru': 'VK',
    'oauth.tau.vk.ru': 'VK',
    'oauth2.cs7777.vk.ru': 'VK',
    'ord.vk.ru': 'VK',
    'push.vk.ru': 'VK',
    'r.vk.ru': 'VK',
    'target.vk.ru': 'VK',
    'tech.vk.ru': 'VK',
    'ui.cs7777.vk.ru': 'VK',
    'ui.tau.vk.ru': 'VK',
    'vkvideo.cs7777.vk.ru': 'VK Видео',
    
    # Speedload - сервис для тестирования скорости
    'speedload.ru': 'Speedload',
    'api.speedload.ru': 'Speedload',
    'chat.speedload.ru': 'Speedload',
    
    # Serverstats
    'serverstats.ru': 'ServerStats',
    'cdnfive.serverstats.ru': 'ServerStats',
    'cdncloudtwo.serverstats.ru': 'ServerStats',
    
    # Furypay
    'furypay.ru': 'FuryPay',
    'api.furypay.ru': 'FuryPay',
    
    # JoJack - сообщество
    'jojack.ru': 'JoJack',
    'spb.jojack.ru': 'JoJack',
    'at.jojack.ru': 'JoJack',
    
    # Tcp-reset-club
    'tcp-reset-club.net': 'TCP Reset',
    'est01-ss01.tcp-reset-club.net': 'TCP Reset',
    'nl01-ss01.tcp-reset-club.net': 'TCP Reset',
    'ru01-blh01.tcp-reset-club.net': 'TCP Reset',
    
    # Государственные
    'gov.ru': 'Госуслуги',
    'kremlin.ru': 'Кремль',
    'government.ru': 'Правительство',
    'duma.gov.ru': 'Госдума',
    'genproc.gov.ru': 'Генпрокуратура',
    'epp.genproc.gov.ru': 'Генпрокуратура',
    'cikrf.ru': 'ЦИК',
    'izbirkom.ru': 'Избирком',
    'gosuslugi.ru': 'Госуслуги',
    'sfd.gosuslugi.ru': 'Госуслуги',
    'esia.gosuslugi.ru': 'Госуслуги',
    'bot.gosuslugi.ru': 'Госуслуги',
    'contract.gosuslugi.ru': 'Госуслуги',
    'novorossiya.gosuslugi.ru': 'Госуслуги',
    'pos.gosuslugi.ru': 'Госуслуги',
    'lk.gosuslugi.ru': 'Госуслуги',
    'map.gosuslugi.ru': 'Госуслуги',
    'partners.gosuslugi.ru': 'Госуслуги',
    'gosweb.gosuslugi.ru': 'Госуслуги',
    'voter.gosuslugi.ru': 'Госуслуги',
    'gu-st.ru': 'Госуслуги',
    'nalog.ru': 'ФНС',
    'pfr.gov.ru': 'ПФР',
    'digital.gov.ru': 'Минцифры',
    'adm.digital.gov.ru': 'Минцифры',
    'xn--80ajghhoc2aj1c8b.xn--p1ai': 'Минцифры',
    
    # NSDI ресурсы
    'a.res-nsdi.ru': 'NSDI',
    'b.res-nsdi.ru': 'NSDI',
    'a.auth-nsdi.ru': 'NSDI',
    'b.auth-nsdi.ru': 'NSDI',
    
    # Одноклассники
    'ok.ru': 'Одноклассники',
    'odnoklassniki.ru': 'Одноклассники',
    'cdn.ok.ru': 'Одноклассники',
    'st.okcdn.ru': 'Одноклассники',
    'st.ok.ru': 'Одноклассники',
    'apiok.ru': 'Одноклассники',
    'jira.apiok.ru': 'Одноклассники',
    'api.ok.ru': 'Одноклассники',
    'm.ok.ru': 'Одноклассники',
    'live.ok.ru': 'Одноклассники',
    'multitest.ok.ru': 'Одноклассники',
    'dating.ok.ru': 'Одноклассники',
    'tamtam.ok.ru': 'Одноклассники',
    '742231.ms.ok.ru': 'Одноклассники',
    
    # Ozon
    'ozon.ru': 'Ozon',
    'www.ozon.ru': 'Ozon',
    'seller.ozon.ru': 'Ozon',
    'bank.ozon.ru': 'Ozon',
    'pay.ozon.ru': 'Ozon',
    'securepay.ozon.ru': 'Ozon',
    'adv.ozon.ru': 'Ozon',
    'invest.ozon.ru': 'Ozon',
    'ord.ozon.ru': 'Ozon',
    'autodiscover.ord.ozon.ru': 'Ozon',
    'st.ozone.ru': 'Ozon',
    'ir.ozone.ru': 'Ozon',
    'vt-1.ozone.ru': 'Ozon',
    'ir-2.ozone.ru': 'Ozon',
    'xapi.ozon.ru': 'Ozon',
    'owa.ozon.ru': 'Ozon',
    'learning.ozon.ru': 'Ozon',
    'mapi.learning.ozon.ru': 'Ozon',
    'ws.seller.ozon.ru': 'Ozon',
    
    # Wildberries
    'wildberries.ru': 'Wildberries',
    'wb.ru': 'Wildberries',
    'static.wb.ru': 'Wildberries',
    'seller.wildberries.ru': 'Wildberries',
    'banners.wildberries.ru': 'Wildberries',
    'fw.wb.ru': 'Wildberries',
    'finance.wb.ru': 'Wildberries',
    'jitsi.wb.ru': 'Wildberries',
    'dnd.wb.ru': 'Wildberries',
    'user-geo-data.wildberries.ru': 'Wildberries',
    'banners-website.wildberries.ru': 'Wildberries',
    'chat-prod.wildberries.ru': 'Wildberries',
    'a.wb.ru': 'Wildberries',
    
    # Avito
    'avito.ru': 'Avito',
    'm.avito.ru': 'Avito',
    'api.avito.ru': 'Avito',
    'avito.st': 'Avito',
    'img.avito.st': 'Avito',
    'sntr.avito.ru': 'Avito',
    'stats.avito.ru': 'Avito',
    'cs.avito.ru': 'Avito',
    'www.avito.st': 'Avito',
    'st.avito.ru': 'Avito',
    'www.avito.ru': 'Avito',
    
    # Avito.st изображения (все 00-99)
    **{f'{i:02d}.img.avito.st': 'Avito' for i in range(100)},
    
    # Банки
    'sberbank.ru': 'Сбербанк',
    'online.sberbank.ru': 'Сбербанк',
    'sber.ru': 'Сбербанк',
    'id.sber.ru': 'Сбербанк',
    'bfds.sberbank.ru': 'Сбербанк',
    'cms-res-web.online.sberbank.ru': 'Сбербанк',
    'esa-res.online.sberbank.ru': 'Сбербанк',
    'pl-res.online.sberbank.ru': 'Сбербанк',
    'www.sberbank.ru': 'Сбербанк',
    'vtb.ru': 'ВТБ',
    'www.vtb.ru': 'ВТБ',
    'online.vtb.ru': 'ВТБ',
    'chat3.vtb.ru': 'ВТБ',
    's.vtb.ru': 'ВТБ',
    'sso-app4.vtb.ru': 'ВТБ',
    'sso-app5.vtb.ru': 'ВТБ',
    'gazprombank.ru': 'Газпромбанк',
    'alfabank.ru': 'Альфа-Банк',
    'metrics.alfabank.ru': 'Альфа-Банк',
    'tinkoff.ru': 'Тинькофф',
    'tbank.ru': 'Тинькофф',
    'cdn.tbank.ru': 'Тинькофф',
    'hrc.tbank.ru': 'Тинькофф',
    'cobrowsing.tbank.ru': 'Тинькофф',
    'le.tbank.ru': 'Тинькофф',
    'id.tbank.ru': 'Тинькофф',
    'imgproxy.cdn-tinkoff.ru': 'Тинькофф',
    'banki.ru': 'Банки.ру',
    
    # Яндекс
    'yandex.ru': 'Яндекс',
    'ya.ru': 'Яндекс',
    'dzen.ru': 'Дзен',
    'kinopoisk.ru': 'Кинопоиск',
    'yastatic.net': 'Яндекс',
    'yandex.net': 'Яндекс',
    'mail.yandex.ru': 'Яндекс Почта',
    'disk.yandex.ru': 'Яндекс Диск',
    'maps.yandex.ru': 'Яндекс Карты',
    'api-maps.yandex.ru': 'Яндекс Карты',
    'enterprise.api-maps.yandex.ru': 'Яндекс Карты',
    'music.yandex.ru': 'Яндекс Музыка',
    'yandex.by': 'Яндекс',
    'yandex.com': 'Яндекс',
    'travel.yandex.ru': 'Яндекс Путешествия',
    'informer.yandex.ru': 'Яндекс',
    'mediafeeds.yandex.ru': 'Яндекс',
    'mediafeeds.yandex.com': 'Яндекс',
    'uslugi.yandex.ru': 'Яндекс Услуги',
    'kiks.yandex.ru': 'Яндекс',
    'kiks.yandex.com': 'Яндекс',
    'frontend.vh.yandex.ru': 'Яндекс',
    'favicon.yandex.ru': 'Яндекс',
    'favicon.yandex.com': 'Яндекс',
    'favicon.yandex.net': 'Яндекс',
    'browser.yandex.ru': 'Яндекс Браузер',
    'browser.yandex.com': 'Яндекс Браузер',
    'api.browser.yandex.ru': 'Яндекс Браузер',
    'api.browser.yandex.com': 'Яндекс Браузер',
    'wap.yandex.ru': 'Яндекс',
    'wap.yandex.com': 'Яндекс',
    '300.ya.ru': 'Яндекс',
    'brontp-pre.yandex.ru': 'Яндекс',
    'suggest.dzen.ru': 'Дзен',
    'suggest.sso.dzen.ru': 'Дзен',
    'sso.dzen.ru': 'Дзен',
    'mail.yandex.com': 'Яндекс Почта',
    'yabs.yandex.ru': 'Яндекс',
    'neuro.translate.yandex.ru': 'Яндекс Перевод',
    'cdn.yandex.ru': 'Яндекс',
    'zen.yandex.ru': 'Дзен',
    'zen.yandex.com': 'Дзен',
    'zen.yandex.net': 'Дзен',
    'collections.yandex.ru': 'Яндекс Коллекции',
    'collections.yandex.com': 'Яндекс Коллекции',
    'an.yandex.ru': 'Яндекс',
    'sba.yandex.ru': 'Яндекс',
    'sba.yandex.com': 'Яндекс',
    'sba.yandex.net': 'Яндекс',
    'surveys.yandex.ru': 'Яндекс Опросы',
    'yabro-wbplugin.edadeal.yandex.ru': 'Яндекс',
    'api.events.plus.yandex.net': 'Яндекс Плюс',
    'speller.yandex.net': 'Яндекс Спеллер',
    'avatars.mds.yandex.net': 'Яндекс',
    'avatars.mds.yandex.com': 'Яндекс',
    'mc.yandex.ru': 'Яндекс',
    'mc.yandex.com': 'Яндекс',
    '3475482542.mc.yandex.ru': 'Яндекс',
    'zen-yabro-morda.mediascope.mc.yandex.ru': 'Яндекс',
    
    # Яндекс CDN и сервисы
    'travel.yastatic.net': 'Яндекс',
    'api.uxfeedback.yandex.net': 'Яндекс',
    'api.s3.yandex.net': 'Яндекс',
    'cdn.s3.yandex.net': 'Яндекс',
    'uxfeedback-cdn.s3.yandex.net': 'Яндекс',
    'uxfeedback.yandex.ru': 'Яндекс',
    'cloudcdn-m9-15.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-14.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-13.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-12.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-10.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-9.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-7.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-6.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-5.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-4.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-3.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-2.cdn.yandex.net': 'Яндекс',
    'cloudcdn-ams19.cdn.yandex.net': 'Яндекс',
    'http-check-headers.yandex.ru': 'Яндекс',
    'cloud.cdn.yandex.net': 'Яндекс',
    'cloud.cdn.yandex.com': 'Яндекс',
    'cloud.cdn.yandex.ru': 'Яндекс',
    'dr2.yandex.net': 'Яндекс',
    'dr.yandex.net': 'Яндекс',
    's3.yandex.net': 'Яндекс',
    'static-mon.yandex.net': 'Яндекс',
    'sync.browser.yandex.net': 'Яндекс',
    'storage.ape.yandex.net': 'Яндекс',
    'strm-rad-23.strm.yandex.net': 'Яндекс',
    'strm.yandex.net': 'Яндекс',
    'strm.yandex.ru': 'Яндекс',
    'log.strm.yandex.ru': 'Яндекс',
    'egress.yandex.net': 'Яндекс',
    'cdnrhkgfkkpupuotntfj.svc.cdn.yandex.net': 'Яндекс',
    'csp.yandex.net': 'Яндекс',
    
    # Mail.ru Group
    'mail.ru': 'Mail.ru',
    'e.mail.ru': 'Mail.ru',
    'my.mail.ru': 'Mail.ru',
    'cloud.mail.ru': 'Mail.ru',
    'inbox.ru': 'Mail.ru',
    'list.ru': 'Mail.ru',
    'bk.ru': 'Mail.ru',
    'myteam.mail.ru': 'Mail.ru',
    'trk.mail.ru': 'Mail.ru',
    '1l-api.mail.ru': 'Mail.ru',
    '1l.mail.ru': 'Mail.ru',
    '1l-s2s.mail.ru': 'Mail.ru',
    '1l-view.mail.ru': 'Mail.ru',
    '1link.mail.ru': 'Mail.ru',
    '1l-hit.mail.ru': 'Mail.ru',
    '2021.mail.ru': 'Mail.ru',
    '2018.mail.ru': 'Mail.ru',
    '23feb.mail.ru': 'Mail.ru',
    '2019.mail.ru': 'Mail.ru',
    '2020.mail.ru': 'Mail.ru',
    '1l-go.mail.ru': 'Mail.ru',
    '8mar.mail.ru': 'Mail.ru',
    '9may.mail.ru': 'Mail.ru',
    'aa.mail.ru': 'Mail.ru',
    '8march.mail.ru': 'Mail.ru',
    'afisha.mail.ru': 'Mail.ru',
    'agent.mail.ru': 'Mail.ru',
    'amigo.mail.ru': 'Mail.ru',
    'analytics.predict.mail.ru': 'Mail.ru',
    'alpha4.minigames.mail.ru': 'Mail.ru',
    'alpha3.minigames.mail.ru': 'Mail.ru',
    'answer.mail.ru': 'Mail.ru',
    'api.predict.mail.ru': 'Mail.ru',
    'answers.mail.ru': 'Mail.ru',
    'authdl.mail.ru': 'Mail.ru',
    'av.mail.ru': 'Mail.ru',
    'apps.research.mail.ru': 'Mail.ru',
    'auto.mail.ru': 'Mail.ru',
    'bb.mail.ru': 'Mail.ru',
    'bender.mail.ru': 'Mail.ru',
    'beko.dom.mail.ru': 'Mail.ru',
    'azt.mail.ru': 'Mail.ru',
    'bd.mail.ru': 'Mail.ru',
    'autodiscover.corp.mail.ru': 'Mail.ru',
    'aw.mail.ru': 'Mail.ru',
    'beta.mail.ru': 'Mail.ru',
    'biz.mail.ru': 'Mail.ru',
    'blackfriday.mail.ru': 'Mail.ru',
    'bitva.mail.ru': 'Mail.ru',
    'blog.mail.ru': 'Mail.ru',
    'bratva-mr.mail.ru': 'Mail.ru',
    'browser.mail.ru': 'Mail.ru',
    'calendar.mail.ru': 'Mail.ru',
    'capsula.mail.ru': 'Mail.ru',
    'cdn.newyear.mail.ru': 'Mail.ru',
    'cars.mail.ru': 'Mail.ru',
    'code.mail.ru': 'Mail.ru',
    'cobmo.mail.ru': 'Mail.ru',
    'cobma.mail.ru': 'Mail.ru',
    'cog.mail.ru': 'Mail.ru',
    'cdn.connect.mail.ru': 'Mail.ru',
    'cf.mail.ru': 'Mail.ru',
    'comba.mail.ru': 'Mail.ru',
    'compute.mail.ru': 'Mail.ru',
    'codefest.mail.ru': 'Mail.ru',
    'combu.mail.ru': 'Mail.ru',
    'corp.mail.ru': 'Mail.ru',
    'commba.mail.ru': 'Mail.ru',
    'crazypanda.mail.ru': 'Mail.ru',
    'ctlog.mail.ru': 'Mail.ru',
    'cpg.money.mail.ru': 'Mail.ru',
    'ctlog2023.mail.ru': 'Mail.ru',
    'ctlog2024.mail.ru': 'Mail.ru',
    'cto.mail.ru': 'Mail.ru',
    'cups.mail.ru': 'Mail.ru',
    'da.biz.mail.ru': 'Mail.ru',
    'da-preprod.biz.mail.ru': 'Mail.ru',
    'data.amigo.mail.ru': 'Mail.ru',
    'dk.mail.ru': 'Mail.ru',
    'dev1.mail.ru': 'Mail.ru',
    'dev3.mail.ru': 'Mail.ru',
    'dl.mail.ru': 'Mail.ru',
    'deti.mail.ru': 'Mail.ru',
    'dn.mail.ru': 'Mail.ru',
    'dl.marusia.mail.ru': 'Mail.ru',
    'doc.mail.ru': 'Mail.ru',
    'dragonpals.mail.ru': 'Mail.ru',
    'dom.mail.ru': 'Mail.ru',
    'duck.mail.ru': 'Mail.ru',
    'dev2.mail.ru': 'Mail.ru',
    'ds.mail.ru': 'Mail.ru',
    'education.mail.ru': 'Mail.ru',
    'dobro.mail.ru': 'Mail.ru',
    'esc.predict.mail.ru': 'Mail.ru',
    'et.mail.ru': 'Mail.ru',
    'fe.mail.ru': 'Mail.ru',
    'finance.mail.ru': 'Mail.ru',
    'five.predict.mail.ru': 'Mail.ru',
    'foto.mail.ru': 'Mail.ru',
    'games-bamboo.mail.ru': 'Mail.ru',
    'games-fisheye.mail.ru': 'Mail.ru',
    'games.mail.ru': 'Mail.ru',
    'genesis.mail.ru': 'Mail.ru',
    'geo-apart.predict.mail.ru': 'Mail.ru',
    'golos.mail.ru': 'Mail.ru',
    'go.mail.ru': 'Mail.ru',
    'gpb.finance.mail.ru': 'Mail.ru',
    'gibdd.mail.ru': 'Mail.ru',
    'health.mail.ru': 'Mail.ru',
    'guns.mail.ru': 'Mail.ru',
    'horo.mail.ru': 'Mail.ru',
    'hs.mail.ru': 'Mail.ru',
    'help.mcs.mail.ru': 'Mail.ru',
    'imperia.mail.ru': 'Mail.ru',
    'it.mail.ru': 'Mail.ru',
    'internet.mail.ru': 'Mail.ru',
    'infra.mail.ru': 'Mail.ru',
    'hi-tech.mail.ru': 'Mail.ru',
    'jd.mail.ru': 'Mail.ru',
    'journey.mail.ru': 'Mail.ru',
    'junior.mail.ru': 'Mail.ru',
    'juggermobile.mail.ru': 'Mail.ru',
    'kicker.mail.ru': 'Mail.ru',
    'knights.mail.ru': 'Mail.ru',
    'kino.mail.ru': 'Mail.ru',
    'kingdomrift.mail.ru': 'Mail.ru',
    'kobmo.mail.ru': 'Mail.ru',
    'komba.mail.ru': 'Mail.ru',
    'kobma.mail.ru': 'Mail.ru',
    'kommba.mail.ru': 'Mail.ru',
    'kombo.mail.ru': 'Mail.ru',
    'kz.mcs.mail.ru': 'Mail.ru',
    'konflikt.mail.ru': 'Mail.ru',
    'kombu.mail.ru': 'Mail.ru',
    'lady.mail.ru': 'Mail.ru',
    'landing.mail.ru': 'Mail.ru',
    'la.mail.ru': 'Mail.ru',
    'legendofheroes.mail.ru': 'Mail.ru',
    'legenda.mail.ru': 'Mail.ru',
    'loa.mail.ru': 'Mail.ru',
    'love.mail.ru': 'Mail.ru',
    'lotro.mail.ru': 'Mail.ru',
    'mailer.mail.ru': 'Mail.ru',
    'mailexpress.mail.ru': 'Mail.ru',
    'man.mail.ru': 'Mail.ru',
    'maps.mail.ru': 'Mail.ru',
    'marusia.mail.ru': 'Mail.ru',
    'mcs.mail.ru': 'Mail.ru',
    'media-golos.mail.ru': 'Mail.ru',
    'mediapro.mail.ru': 'Mail.ru',
    'merch-cpg.money.mail.ru': 'Mail.ru',
    'miniapp.internal.myteam.mail.ru': 'Mail.ru',
    'media.mail.ru': 'Mail.ru',
    'mobfarm.mail.ru': 'Mail.ru',
    'mowar.mail.ru': 'Mail.ru',
    'mozilla.mail.ru': 'Mail.ru',
    'mosqa.mail.ru': 'Mail.ru',
    'mking.mail.ru': 'Mail.ru',
    'minigames.mail.ru': 'Mail.ru',
    'nebogame.mail.ru': 'Mail.ru',
    'money.mail.ru': 'Mail.ru',
    'net.mail.ru': 'Mail.ru',
    'new.mail.ru': 'Mail.ru',
    'newyear2018.mail.ru': 'Mail.ru',
    'news.mail.ru': 'Mail.ru',
    'newyear.mail.ru': 'Mail.ru',
    'nonstandard.sales.mail.ru': 'Mail.ru',
    'notes.mail.ru': 'Mail.ru',
    'octavius.mail.ru': 'Mail.ru',
    'operator.mail.ru': 'Mail.ru',
    'otvety.mail.ru': 'Mail.ru',
    'otvet.mail.ru': 'Mail.ru',
    'otveti.mail.ru': 'Mail.ru',
    'panzar.mail.ru': 'Mail.ru',
    'park.mail.ru': 'Mail.ru',
    'pernatsk.mail.ru': 'Mail.ru',
    'pets.mail.ru': 'Mail.ru',
    'pms.mail.ru': 'Mail.ru',
    'pochtabank.mail.ru': 'Mail.ru',
    'pokerist.mail.ru': 'Mail.ru',
    'pogoda.mail.ru': 'Mail.ru',
    'polis.mail.ru': 'Mail.ru',
    'primeworld.mail.ru': 'Mail.ru',
    'pp.mail.ru': 'Mail.ru',
    'ptd.predict.mail.ru': 'Mail.ru',
    'public.infra.mail.ru': 'Mail.ru',
    'pulse.mail.ru': 'Mail.ru',
    'pubg.mail.ru': 'Mail.ru',
    'quantum.mail.ru': 'Mail.ru',
    'rate.mail.ru': 'Mail.ru',
    'pw.mail.ru': 'Mail.ru',
    'rebus.calls.mail.ru': 'Mail.ru',
    'rebus.octavius.mail.ru': 'Mail.ru',
    'rev.mail.ru': 'Mail.ru',
    'rl.mail.ru': 'Mail.ru',
    'rm.mail.ru': 'Mail.ru',
    'riot.mail.ru': 'Mail.ru',
    'reseach.mail.ru': 'Mail.ru',
    's3.babel.mail.ru': 'Mail.ru',
    'rt.api.operator.mail.ru': 'Mail.ru',
    's3.mail.ru': 'Mail.ru',
    's3.media-mobs.mail.ru': 'Mail.ru',
    'sales.mail.ru': 'Mail.ru',
    'sangels.mail.ru': 'Mail.ru',
    'sdk.money.mail.ru': 'Mail.ru',
    'service.amigo.mail.ru': 'Mail.ru',
    'security.mail.ru': 'Mail.ru',
    'shadowbound.mail.ru': 'Mail.ru',
    'socdwar.mail.ru': 'Mail.ru',
    'sochi-park.predict.mail.ru': 'Mail.ru',
    'souz.mail.ru': 'Mail.ru',
    'sphere.mail.ru': 'Mail.ru',
    'staging-analytics.predict.mail.ru': 'Mail.ru',
    'staging-sochi-park.predict.mail.ru': 'Mail.ru',
    'staging-esc.predict.mail.ru': 'Mail.ru',
    'stand.bb.mail.ru': 'Mail.ru',
    'sport.mail.ru': 'Mail.ru',
    'stand.aoc.mail.ru': 'Mail.ru',
    'stand.cb.mail.ru': 'Mail.ru',
    'startrek.mail.ru': 'Mail.ru',
    'static.dl.mail.ru': 'Mail.ru',
    'stand.pw.mail.ru': 'Mail.ru',
    'stand.la.mail.ru': 'Mail.ru',
    'stormriders.mail.ru': 'Mail.ru',
    'static.operator.mail.ru': 'Mail.ru',
    'stream.mail.ru': 'Mail.ru',
    'status.mcs.mail.ru': 'Mail.ru',
    'street-combats.mail.ru': 'Mail.ru',
    'support.biz.mail.ru': 'Mail.ru',
    'support.mcs.mail.ru': 'Mail.ru',
    'team.mail.ru': 'Mail.ru',
    'support.tech.mail.ru': 'Mail.ru',
    'tech.mail.ru': 'Mail.ru',
    'tera.mail.ru': 'Mail.ru',
    'tiles.maps.mail.ru': 'Mail.ru',
    'todo.mail.ru': 'Mail.ru',
    'tidaltrek.mail.ru': 'Mail.ru',
    'tmgame.mail.ru': 'Mail.ru',
    'townwars.mail.ru': 'Mail.ru',
    'tv.mail.ru': 'Mail.ru',
    'ttbh.mail.ru': 'Mail.ru',
    'typewriter.mail.ru': 'Mail.ru',
    'u.corp.mail.ru': 'Mail.ru',
    'ufo.mail.ru': 'Mail.ru',
    'vkdoc.mail.ru': 'Mail.ru',
    'vk.mail.ru': 'Mail.ru',
    'voina.mail.ru': 'Mail.ru',
    'warface.mail.ru': 'Mail.ru',
    'wartune.mail.ru': 'Mail.ru',
    'weblink.predict.mail.ru': 'Mail.ru',
    'warheaven.mail.ru': 'Mail.ru',
    'welcome.mail.ru': 'Mail.ru',
    'webstore.mail.ru': 'Mail.ru',
    'webagent.mail.ru': 'Mail.ru',
    'wf.mail.ru': 'Mail.ru',
    'whatsnew.mail.ru': 'Mail.ru',
    'wh-cpg.money.mail.ru': 'Mail.ru',
    'wok.mail.ru': 'Mail.ru',
    'www.biz.mail.ru': 'Mail.ru',
    'wos.mail.ru': 'Mail.ru',
    'www.mail.ru': 'Mail.ru',
    'www.pubg.mail.ru': 'Mail.ru',
    'www.wf.mail.ru': 'Mail.ru',
    'www.mcs.mail.ru': 'Mail.ru',
    'rs.mail.ru': 'Mail.ru',
    'top-fwz1.mail.ru': 'Mail.ru',
    'privacy-cs.mail.ru': 'Mail.ru',
    'r0.mradx.net': 'Mail.ru',
    
    # Медиа и новости
    'rutube.ru': 'Rutube',
    'static.rutube.ru': 'Rutube',
    'rutubelist.ru': 'Rutube',
    'pic.rutubelist.ru': 'Rutube',
    'ssp.rutube.ru': 'Rutube',
    'preview.rutube.ru': 'Rutube',
    'goya.rutube.ru': 'Rutube',
    'smotrim.ru': 'Смотрим',
    'ivi.ru': 'Ivi',
    'cdn.ivi.ru': 'Ivi',
    'okko.tv': 'Okko',
    'start.ru': 'Start',
    'wink.ru': 'Wink',
    'kion.ru': 'Kion',
    'premier.one': 'Premier',
    'more.tv': 'More.tv',
    'm.47news.ru': '47 News',
    'lenta.ru': 'Лента.ру',
    'gazeta.ru': 'Газета.ру',
    'kp.ru': 'Комсомольская Правда',
    'rambler.ru': 'Рамблер',
    'ria.ru': 'РИА Новости',
    'tass.ru': 'ТАСС',
    'interfax.ru': 'Интерфакс',
    'kommersant.ru': 'Коммерсант',
    'vedomosti.ru': 'Ведомости',
    'rbc.ru': 'РБК',
    'russian.rt.com': 'RT',
    'iz.ru': 'Известия',
    'mk.ru': 'Московский Комсомолец',
    'rg.ru': 'Российская Газета',
    
    # Кинопоиск
    'www.kinopoisk.ru': 'Кинопоиск',
    'widgets.kinopoisk.ru': 'Кинопоиск',
    'payment-widget.plus.kinopoisk.ru': 'Кинопоиск',
    'external-api.mediabilling.kinopoisk.ru': 'Кинопоиск',
    'external-api.plus.kinopoisk.ru': 'Кинопоиск',
    'graphql-web.kinopoisk.ru': 'Кинопоиск',
    'graphql.kinopoisk.ru': 'Кинопоиск',
    'tickets.widget.kinopoisk.ru': 'Кинопоиск',
    'st.kinopoisk.ru': 'Кинопоиск',
    'quiz.kinopoisk.ru': 'Кинопоиск',
    'payment-widget.kinopoisk.ru': 'Кинопоиск',
    'payment-widget-smarttv.plus.kinopoisk.ru': 'Кинопоиск',
    'oneclick-payment.kinopoisk.ru': 'Кинопоиск',
    'microapps.kinopoisk.ru': 'Кинопоиск',
    'ma.kinopoisk.ru': 'Кинопоиск',
    'hd.kinopoisk.ru': 'Кинопоиск',
    'crowdtest.payment-widget-smarttv.plus.tst.kinopoisk.ru': 'Кинопоиск',
    'crowdtest.payment-widget.plus.tst.kinopoisk.ru': 'Кинопоиск',
    'api.plus.kinopoisk.ru': 'Кинопоиск',
    'st-im.kinopoisk.ru': 'Кинопоиск',
    'sso.kinopoisk.ru': 'Кинопоиск',
    'touch.kinopoisk.ru': 'Кинопоиск',
    
    # Карты и транспорт
    '2gis.ru': '2ГИС',
    '2gis.com': '2ГИС',
    'api.2gis.ru': '2ГИС',
    'keys.api.2gis.com': '2ГИС',
    'favorites.api.2gis.com': '2ГИС',
    'styles.api.2gis.com': '2ГИС',
    'tile0.maps.2gis.com': '2ГИС',
    'tile1.maps.2gis.com': '2ГИС',
    'tile2.maps.2gis.com': '2ГИС',
    'tile3.maps.2gis.com': '2ГИС',
    'tile4.maps.2gis.com': '2ГИС',
    'api.photo.2gis.com': '2ГИС',
    'filekeeper-vod.2gis.com': '2ГИС',
    'i0.photo.2gis.com': '2ГИС',
    'i1.photo.2gis.com': '2ГИС',
    'i2.photo.2gis.com': '2ГИС',
    'i3.photo.2gis.com': '2ГИС',
    'i4.photo.2gis.com': '2ГИС',
    'i5.photo.2gis.com': '2ГИС',
    'i6.photo.2gis.com': '2ГИС',
    'i7.photo.2gis.com': '2ГИС',
    'i8.photo.2gis.com': '2ГИС',
    'i9.photo.2gis.com': '2ГИС',
    'jam.api.2gis.com': '2ГИС',
    'catalog.api.2gis.com': '2ГИС',
    'api.reviews.2gis.com': '2ГИС',
    'public-api.reviews.2gis.com': '2ГИС',
    'mapgl.2gis.com': '2ГИС',
    'd-assets.2gis.ru': '2ГИС',
    'disk.2gis.com': '2ГИС',
    's0.bss.2gis.com': '2ГИС',
    's1.bss.2gis.com': '2ГИС',
    'ams2-cdn.2gis.com': '2ГИС',
    'tutu.ru': 'Туту.ру',
    'img.tutu.ru': 'Туту.ру',
    'rzd.ru': 'РЖД',
    'ticket.rzd.ru': 'РЖД',
    'pass.rzd.ru': 'РЖД',
    'cargo.rzd.ru': 'РЖД',
    'company.rzd.ru': 'РЖД',
    'contacts.rzd.ru': 'РЖД',
    'team.rzd.ru': 'РЖД',
    'my.rzd.ru': 'РЖД',
    'prodvizhenie.rzd.ru': 'РЖД',
    'disk.rzd.ru': 'РЖД',
    'market.rzd.ru': 'РЖД',
    'secure.rzd.ru': 'РЖД',
    'secure-cloud.rzd.ru': 'РЖД',
    'travel.rzd.ru': 'РЖД',
    'welcome.rzd.ru': 'РЖД',
    'adm.mp.rzd.ru': 'РЖД',
    'link.mp.rzd.ru': 'РЖД',
    'pulse.mp.rzd.ru': 'РЖД',
    'mp.rzd.ru': 'РЖД',
    'ekmp-a-51.rzd.ru': 'РЖД',
    'cdek.ru': 'СДЭК',
    'cdek.market': 'СДЭК',
    'calc.cdek.ru': 'СДЭК',
    'pochta.ru': 'Почта России',
    'ws-api.oneme.ru': 'Oneme',
    
    # Телеком
    'rostelecom.ru': 'Ростелеком',
    'rt.ru': 'Ростелеком',
    'mts.ru': 'МТС',
    'megafon.ru': 'Мегафон',
    'beeline.ru': 'Билайн',
    'tele2.ru': 'Tele2',
    't2.ru': 'Tele2',
    'www.t2.ru': 'Tele2',
    'msk.t2.ru': 'Tele2',
    's3.t2.ru': 'Tele2',
    'yota.ru': 'Yota',
    'domru.ru': 'Дом.ру',
    'ertelecom.ru': 'ЭР-Телеком',
    'selectel.ru': 'Selectel',
    'timeweb.ru': 'Timeweb',
    
    # Погода
    'gismeteo.ru': 'Гисметео',
    'meteoinfo.ru': 'Метео',
    'rp5.ru': 'RП5',
    
    # Работа
    'hh.ru': 'HeadHunter',
    'superjob.ru': 'SuperJob',
    'rabota.ru': 'Работа.ру',
    
    # Авто
    'auto.ru': 'Auto.ru',
    'sso.auto.ru': 'Auto.ru',
    'drom.ru': 'Drom',
    'avto.ru': 'Avto.ru',
    
    # Еда
    'eda.ru': 'Eda.ru',
    'food.ru': 'Food.ru',
    'edadeal.ru': 'Edadeal',
    'delivery-club.ru': 'Delivery Club',
    
    # Дом и стройка
    'leroymerlin.ru': 'Леруа Мерлен',
    'lemanapro.ru': 'Лемана Про',
    'cdn.lemanapro.ru': 'Лемана Про',
    'static.lemanapro.ru': 'Лемана Про',
    'dmp.dmpkit.lemanapro.ru': 'Лемана Про',
    'receive-sentry.lmru.tech': 'Лемана Про',
    'partners.lemanapro.ru': 'Лемана Про',
    'petrovich.ru': 'Петрович',
    'maxidom.ru': 'Максидом',
    'vseinstrumenti.ru': 'ВсеИнструменты',
    '220-volt.ru': '220 Вольт',
    
    # Max
    'max.ru': 'Max',
    'dev.max.ru': 'Max',
    'web.max.ru': 'Max',
    'api.max.ru': 'Max',
    'legal.max.ru': 'Max',
    'st.max.ru': 'Max',
    'botapi.max.ru': 'Max',
    'link.max.ru': 'Max',
    'download.max.ru': 'Max',
    'i.max.ru': 'Max',
    'help.max.ru': 'Max',
    
    # Прочее
    'mos.ru': 'Мос.ру',
    'taximaxim.ru': 'Такси Максим',
    'moskva.taximaxim.ru': 'Такси Максим',
}

print(f"📋 Загружено {len(DOMAIN_NAMES)} доменов в словарь")

# ========= ЛОГ =========
async def log(message: str):
    try:
        now = datetime.now()
        if os.path.exists(LOG_FILE):
            mtime = datetime.fromtimestamp(os.path.getmtime(LOG_FILE))
            if now - mtime > timedelta(seconds=LOG_CLEAN_INTERVAL):
                open(LOG_FILE, "w").close()

        async with aiofiles.open(LOG_FILE, "a", encoding="utf-8") as f:
            await f.write(f"[{now}] {message}\n")
    except Exception:
        pass

def log_xray_error(message: str):
    """Логирует ошибки Xray"""
    try:
        with open(XRAY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")
    except Exception:
        pass

# ========= ВАЛИДАЦИЯ =========
def validate_vless(url: str) -> bool:
    if not url.startswith("vless://"):
        return False
    if not UUID_REGEX.search(url):
        return False
    if "@" not in url:
        return False
    if ":" not in url:
        return False
    return True


def validate_vmess(url: str) -> bool:
    if not url.lower().startswith("vmess://"):
        return False
    try:
        b64 = url[8:].split('#')[0].strip()
        padding = 4 - len(b64) % 4
        if padding != 4:
            b64 += '=' * padding
        try:
            decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
        except Exception:
            decoded = base64.urlsafe_b64decode(b64).decode('utf-8', errors='ignore')
        data = json.loads(decoded)
        return bool(data.get('add') and data.get('port') and data.get('id'))
    except Exception:
        return False


def validate_trojan(url: str) -> bool:
    if not url.lower().startswith("trojan://"):
        return False
    content = url[9:].split('#')[0]
    if '@' not in content:
        return False
    after_at = content.split('@', 1)[1].split('?')[0]
    return ':' in after_at


def validate_ss(url: str) -> bool:
    if not url.lower().startswith("ss://"):
        return False
    content = url[5:].split('#')[0]
    return len(content) > 5


def validate_hysteria2(url: str) -> bool:
    url_lower = url.lower()
    if not (url_lower.startswith("hysteria2://") or url_lower.startswith("hy2://")):
        return False
    content = url[12:] if url_lower.startswith("hysteria2://") else url[6:]
    return len(content) > 3


def validate_config(url: str) -> bool:
    """Validates any supported protocol config URL"""
    if not url or not url.strip():
        return False
    proto = get_protocol_type(url)
    if proto == 'vless':
        return validate_vless(url)
    elif proto == 'vmess':
        return validate_vmess(url)
    elif proto == 'trojan':
        return validate_trojan(url)
    elif proto == 'ss':
        return validate_ss(url)
    elif proto == 'hysteria2':
        return validate_hysteria2(url)
    return False


def get_protocol_type(url: str) -> str:
    """Returns the base protocol type: vless, vmess, trojan, ss, hysteria2, or unknown"""
    url_lower = url.lower()
    if url_lower.startswith('vless://'):
        return 'vless'
    elif url_lower.startswith('vmess://'):
        return 'vmess'
    elif url_lower.startswith('trojan://'):
        return 'trojan'
    elif url_lower.startswith('ss://'):
        return 'ss'
    elif url_lower.startswith('hysteria2://') or url_lower.startswith('hy2://'):
        return 'hysteria2'
    return 'unknown'

# ========= WHITELIST =========
def load_whitelist_domains():
    domains = set()
    suffixes = []

    if os.path.exists("whitelist.txt"):
        try:
            with open("whitelist.txt", "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    d = line.strip().lower()
                    if not d:
                        continue
                    domains.add(d)
                    suffixes.append("." + d)
            print(f"📋 Загружено {len(domains)} доменов из whitelist.txt")
        except Exception:
            print("⚠️ Ошибка загрузки whitelist.txt")

    return domains, suffixes


# ========= ПРОТОКОЛ / SNI / НАЗВАНИЕ =========
def detect_protocol(vless_url: str) -> str:
    try:
        no_scheme = vless_url[len("vless://"):]
        after_at = no_scheme.split("@", 1)[1]
        query = after_at.split("?", 1)[1] if "?" in after_at else ""
        params = {}

        for part in query.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                params[k.lower()] = v.lower()

        transport = params.get("type", "").lower()
        security = params.get("security", "").lower()

        if transport in ("ws", "websocket"):
            return "WS"
        if transport in ("grpc", "gun"):
            return "gRPC"
        if transport in ("xhttp", "httpupgrade"):
            return "XHTTP"
        if transport in ("h2", "http2"):
            return "H2"
        if transport == "tcp":
            return "TCP"
        if security == "reality":
            return "Reality"
        if security in ("tls", "xtls"):
            return "TLS"
        return "TCP"
    except Exception:
        return "Неизвестно"


def extract_sni_or_host(vless_url: str) -> Optional[str]:
    """Извлекает SNI или host из VLESS URL"""
    try:
        if not vless_url.startswith("vless://"):
            return None
            
        content = vless_url[8:]
        at_pos = content.find('@')
        if at_pos == -1:
            return None
            
        after_at = content[at_pos+1:]
        
        q_pos = after_at.find('?')
        if q_pos != -1:
            host_part = after_at[:q_pos]
            query = after_at[q_pos+1:]
        else:
            host_part = after_at
            query = ""
        
        hash_pos = host_part.find('#')
        if hash_pos != -1:
            host_part = host_part[:hash_pos]
        
        if ':' in host_part:
            host = host_part.split(':', 1)[0]
        else:
            host = host_part
        
        sni = None
        if query:
            for param in query.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    if k.lower() == 'sni' or k.lower() == 'host':
                        sni = v
        
        return sni or host
    except Exception:
        return None


def extract_all_possible_domains(vless_url: str) -> list:
    """
    Извлекает все возможные домены из VLESS URL
    """
    domains = set()
    
    try:
        if not vless_url.startswith("vless://"):
            return []
        
        content = vless_url[8:]
        
        at_pos = content.find('@')
        if at_pos == -1:
            return []
        
        after_at = content[at_pos+1:]
        
        q_pos = after_at.find('?')
        if q_pos != -1:
            host_part = after_at[:q_pos]
            query_part = after_at[q_pos+1:]
        else:
            host_part = after_at
            query_part = ""
        
        if ':' in host_part:
            host = host_part.split(':', 1)[0]
        else:
            host = host_part
        
        if host and '.' in host:
            domains.add(host.lower())
        
        if query_part:
            if '#' in query_part:
                query_part = query_part.split('#', 1)[0]
            
            for param in query_part.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    try:
                        v_decoded = urllib.parse.unquote(v).lower()
                    except Exception:
                        v_decoded = v.lower()
                    
                    if k.lower() == 'sni' and '.' in v_decoded:
                        domains.add(v_decoded)
                    
                    elif k.lower() == 'host' and '.' in v_decoded:
                        domains.add(v_decoded)
                    
                    elif k.lower() == 'path':
                        path_parts = re.findall(r'[a-zA-Z0-9][a-zA-Z0-9\-\.]+[a-zA-Z0-9]\.[a-zA-Z]{2,}', v_decoded)
                        for d in path_parts:
                            domains.add(d.lower())
        
        if '#' in after_at:
            fragment = after_at.split('#', 1)[1]
            try:
                fragment_decoded = urllib.parse.unquote(fragment)
                domain_in_frag = re.findall(r'[a-zA-Z0-9][a-zA-Z0-9\-\.]+[a-zA-Z0-9]\.[a-zA-Z]{2,}', fragment_decoded)
                for d in domain_in_frag:
                    domains.add(d.lower())
            except Exception:
                pass
        
        return list(domains)
    except Exception as e:
        return []


def extract_vmess_domains(url: str) -> list:
    """Extracts domains from VMess URL (base64-encoded JSON)"""
    domains = set()
    try:
        if not url.lower().startswith('vmess://'):
            return []
        b64 = url[8:].split('#')[0].strip()
        padding = 4 - len(b64) % 4
        if padding != 4:
            b64 += '=' * padding
        try:
            decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
        except Exception:
            decoded = base64.urlsafe_b64decode(b64).decode('utf-8', errors='ignore')
        data = json.loads(decoded)
        for field in ('sni', 'host', 'add'):
            val = str(data.get(field, '') or '').lower()
            if val and '.' in val:
                domains.add(val)
    except Exception:
        pass
    return list(domains)


def extract_trojan_domains(url: str) -> list:
    """Extracts domains from Trojan URL"""
    domains = set()
    try:
        if not url.lower().startswith('trojan://'):
            return []
        content = url[9:].split('#')[0]
        if '@' not in content:
            return []
        after_at = content.split('@', 1)[1]
        if '?' in after_at:
            host_part, query = after_at.split('?', 1)
        else:
            host_part = after_at
            query = ''
        host = host_part.split(':', 1)[0] if ':' in host_part else host_part
        if host and '.' in host:
            domains.add(host.lower())
        if query:
            for param in query.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    try:
                        v = urllib.parse.unquote(v)
                    except Exception:
                        pass
                    if k.lower() in ('sni', 'peer') and '.' in v:
                        domains.add(v.lower())
    except Exception:
        pass
    return list(domains)


def extract_ss_domains(url: str) -> list:
    """Extracts domains from Shadowsocks URL"""
    domains = set()
    try:
        if not url.lower().startswith('ss://'):
            return []
        content = url[5:].split('#')[0]
        if '@' in content:
            after_at = content.split('@', 1)[1]
            host_part = after_at.split('?', 1)[0]
            host = host_part.rsplit(':', 1)[0] if ':' in host_part else host_part
            if host and '.' in host:
                domains.add(host.lower())
        else:
            b64 = content
            padding = 4 - len(b64) % 4
            if padding != 4:
                b64 += '=' * padding
            try:
                decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
                if '@' in decoded:
                    after_at = decoded.split('@', 1)[1]
                    host = after_at.rsplit(':', 1)[0] if ':' in after_at else after_at
                    if host and '.' in host:
                        domains.add(host.lower())
            except Exception:
                pass
    except Exception:
        pass
    return list(domains)


def extract_hysteria2_domains(url: str) -> list:
    """Extracts domains from Hysteria2 URL"""
    domains = set()
    try:
        url_lower = url.lower()
        if url_lower.startswith('hysteria2://'):
            content = url[12:]
        elif url_lower.startswith('hy2://'):
            content = url[6:]
        else:
            return []
        content = content.split('#')[0]
        after_at = content.split('@', 1)[1] if '@' in content else content
        if '?' in after_at:
            host_part, query = after_at.split('?', 1)
        else:
            host_part = after_at
            query = ''
        host = host_part.rsplit(':', 1)[0] if ':' in host_part else host_part
        if host and '.' in host:
            domains.add(host.lower())
        if query:
            for param in query.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    try:
                        v = urllib.parse.unquote(v)
                    except Exception:
                        pass
                    if k.lower() == 'sni' and '.' in v:
                        domains.add(v.lower())
    except Exception:
        pass
    return list(domains)


def extract_all_possible_domains_generic(url: str) -> list:
    """Generic domain extractor that dispatches to the right protocol handler"""
    proto = get_protocol_type(url)
    if proto == 'vless':
        return extract_all_possible_domains(url)
    elif proto == 'vmess':
        return extract_vmess_domains(url)
    elif proto == 'trojan':
        return extract_trojan_domains(url)
    elif proto == 'ss':
        return extract_ss_domains(url)
    elif proto == 'hysteria2':
        return extract_hysteria2_domains(url)
    return []


def get_human_name(domain: str) -> str:
    """Определяет человеко-читаемое название по домену из словаря DOMAIN_NAMES"""
    if not domain:
        return "Неизвестно"

    d = domain.lower()
    parts = d.split('.')

    # Проверяем от полного домена до базового (sub.example.com → example.com)
    for i in range(len(parts) - 1):
        sub = ".".join(parts[i:])
        if sub in DOMAIN_NAMES:
            return DOMAIN_NAMES[sub]

    return "Неизвестно"


def detect_protocol_label(url: str) -> str:
    """Returns a human-readable protocol label for config renaming (e.g. WS, VMess, HY2)"""
    proto = get_protocol_type(url)
    if proto == 'vless':
        return detect_protocol(url)
    elif proto == 'vmess':
        return 'VMess'
    elif proto == 'trojan':
        return 'Trojan'
    elif proto == 'ss':
        return 'SS'
    elif proto == 'hysteria2':
        return 'HY2'
    return 'Unknown'


def filter_by_sni(vless_url: str, whitelist_domains: set, whitelist_suffixes: list) -> bool:
    """
    Фильтрация по SNI с использованием whitelist и DOMAIN_NAMES
    """
    # Используем универсальный экстрактор доменов для всех протоколов
    domains = extract_all_possible_domains_generic(vless_url)
    
    # Проверяем по whitelist
    for domain in domains:
        if domain in whitelist_domains:
            return True

        for suffix in whitelist_suffixes:
            if domain.endswith(suffix):
                return True

        parts = domain.split('.')
        if len(parts) >= 2:
            base_domain = '.'.join(parts[-2:])
            if base_domain in whitelist_domains:
                return True

    # Если whitelist пуст, используем DOMAIN_NAMES
    if not whitelist_domains:
        for domain in domains:
            parts = domain.split('.')
            for i in range(len(parts) - 1):
                sub = ".".join(parts[i:])
                if sub in DOMAIN_NAMES:
                    return True

    return False


# ========= СКАЧИВАНИЕ =========
async def fetch(session, url, sem):
    async with sem:
        try:
            print(f"Скачиваю: {url}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception as e:
            await log(f"Ошибка при скачивании {url}: {e}")
    return None


async def process_url(session, url, sem, output_lock, stats):
    content = await fetch(session, url, sem)
    stats["processed"] += 1

    if not content:
        return

    all_matches = []
    for regex in PROTOCOL_REGEXES.values():
        all_matches.extend(regex.findall(content))

    if all_matches:
        async with output_lock:
            async with aiofiles.open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                for m in all_matches:
                    await f.write(m + "\n")
        stats["found"] += len(all_matches)

    print(f"Обработано: {stats['processed']} | Найдено конфигов: {stats['found']}", end="\r")


# ========= ОЧИСТКА =========
async def clean_vless():
    print("\nОчищаю дубликаты и проверяю валидность...")

    if not os.path.exists(OUTPUT_FILE):
        print("Нет файла url.txt — пропускаю очистку.")
        return

    try:
        async with aiofiles.open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            lines = await f.readlines()
    except Exception:
        print("Ошибка чтения файла")
        return

    unique = set()
    valid = []

    for line in lines:
        url = line.strip()
        if not url or not validate_config(url):
            continue
        # Deduplicate by URL without the fragment (name) part
        url_key = url.split("#", 1)[0]
        if url_key not in unique:
            unique.add(url_key)
            valid.append(url)

    async with aiofiles.open(CLEAN_FILE, "w", encoding="utf-8") as f:
        for url in valid:
            await f.write(url + "\n")

    print(f"Очистка завершена. Итоговых конфигов: {len(valid)}")


# ========= ФИЛЬТРАЦИЯ ПО WHITELIST =========
async def filter_vless():
    print("\n=== Фильтрация по whitelist ===")

    if not os.path.exists(CLEAN_FILE):
        print("Нет файла url_clean.txt — пропускаю фильтрацию.")
        return

    domains, suffixes = load_whitelist_domains()

    try:
        with open(CLEAN_FILE, "r", encoding="utf-8", errors="ignore") as f:
            total = sum(1 for _ in f)
    except Exception:
        total = 0
        
    passed = 0
    processed = 0

    async with aiofiles.open(CLEAN_FILE, "r", encoding="utf-8") as f_in, \
               aiofiles.open(FILTERED_FILE, "w", encoding="utf-8") as f_out:

        async for line in f_in:
            processed += 1
            url = line.strip()
            if not url:
                continue

            if filter_by_sni(url, domains, suffixes):
                await f_out.write(url + "\n")
                passed += 1

            if processed % 100 == 0 and total > 0:
                print(f"Фильтрация: {processed}/{total} | Подошло: {passed}", end="\r")

    print(f"\nФильтрация завершена. Итог: {passed} конфигов.")


# ========= ПЕРЕИМЕНОВАНИЕ =========
async def rename_configs():
    print("\n=== Переименование конфигов по протоколу и SNI ===")

    if not os.path.exists(FILTERED_FILE):
        print("Нет файла url_filtered.txt — пропускаю переименование.")
        return

    try:
        with open(FILTERED_FILE, "r", encoding="utf-8", errors="ignore") as f:
            total = sum(1 for _ in f)
    except Exception:
        total = 0
        
    processed = 0

    async with aiofiles.open(FILTERED_FILE, "r", encoding="utf-8") as f_in, \
               aiofiles.open(NAMED_FILE, "w", encoding="utf-8") as f_out:

        async for line in f_in:
            processed += 1
            url = line.strip()
            if not url:
                continue

            protocol = detect_protocol_label(url)
            
            domains = extract_all_possible_domains_generic(url)
            human_name = "Неизвестно"
            
            if domains:
                for domain in domains:
                    name = get_human_name(domain)
                    if name != "Неизвестно":
                        human_name = name
                        break
                if human_name == "Неизвестно" and domains:
                    human_name = domains[0].split('.')[-2].capitalize() if len(domains[0].split('.')) >= 2 else domains[0]

            title = f"{protocol}, {human_name} [#РКП]"
            base = url.split("#", 1)[0]
            new_url = f"{base}#{title}"

            await f_out.write(new_url + "\n")

            if processed % 500 == 0 and total > 0:
                print(f"Переименовано: {processed}/{total}", end="\r")

    print(f"\nПереименование завершено. Итог: {processed} конфигов.")


# ========= НОРМАЛИЗАЦИЯ И КОДИРОВАНИЕ URL =========
def encode_vless_url(url: str) -> str:
    """
    Нормализует и кодирует VLESS URL
    """
    try:
        if not url.startswith("vless://"):
            return url
        
        content = url[8:]
        at_pos = content.find('@')
        if at_pos == -1:
            return url
        
        uuid = content[:at_pos]
        after_at = content[at_pos+1:]
        
        q_pos = after_at.find('?')
        if q_pos != -1:
            host_part = after_at[:q_pos]
            params_part = after_at[q_pos+1:]
        else:
            host_part = after_at
            params_part = ""
        
        hash_pos = host_part.find('#')
        if hash_pos != -1:
            host_only = host_part[:hash_pos]
            fragment = host_part[hash_pos+1:]
        else:
            host_only = host_part
            fragment = ""
        
        if not fragment and params_part:
            hash_pos = params_part.find('#')
            if hash_pos != -1:
                params_only = params_part[:hash_pos]
                fragment = params_part[hash_pos+1:]
                params_part = params_only
        
        params = {}
        if params_part:
            for param in params_part.split('&'):
                if '=' in param:
                    k, v = param.split('=', 1)
                    params[k] = v
        
        encoded_params = []
        for k, v in params.items():
            if k in ['security', 'type', 'fp', 'pbk', 'sid', 'flow']:
                encoded_params.append(f"{k}={v}")
            else:
                try:
                    encoded_params.append(f"{k}={urllib.parse.quote(v, safe='')}")
                except Exception:
                    encoded_params.append(f"{k}={v}")
        
        new_params = "&".join(encoded_params) if encoded_params else ""
        
        if fragment:
            try:
                if any(ord(c) > 127 for c in fragment):
                    encoded_fragment = urllib.parse.quote(fragment, safe='')
                else:
                    encoded_fragment = fragment
            except Exception:
                encoded_fragment = fragment
        else:
            encoded_fragment = ""
        
        base = f"vless://{uuid}@{host_only}"
        if new_params:
            base += f"?{new_params}"
        if encoded_fragment:
            base += f"#{encoded_fragment}"
        
        return base
        
    except Exception as e:
        return url


def encode_config_url(url: str) -> str:
    """Encodes/normalizes a config URL for any protocol"""
    proto = get_protocol_type(url)
    if proto == 'vless':
        return encode_vless_url(url)
    # For other protocols: ensure the fragment (name) is properly percent-encoded
    if '#' in url:
        base, fragment = url.split('#', 1)
        try:
            if any(ord(c) > 127 for c in fragment):
                fragment = urllib.parse.quote(fragment, safe='')
        except Exception:
            pass
        return f"{base}#{fragment}"
    return url


async def encode_all_configs():
    """
    Кодирует все конфиги для лучшей совместимости с Xray
    """
    print("\n=== Кодирование конфигов для Xray ===")
    
    if not os.path.exists(NAMED_FILE):
        print("Нет файла url_named.txt — пропускаю кодирование.")
        return
    
    try:
        with open(NAMED_FILE, 'r', encoding='utf-8') as f:
            configs = [line.strip() for line in f if line.strip()]
    except Exception:
        print("Ошибка чтения файла")
        return
    
    total = len(configs)
    changed = 0
    
    async with aiofiles.open(ENCODED_FILE, "w", encoding="utf-8") as f_out:
        for i, url in enumerate(configs, 1):
            encoded_url = encode_config_url(url)
            await f_out.write(encoded_url + "\n")
            
            if encoded_url != url:
                changed += 1
            
            if i % 500 == 0:
                print(f"Закодировано: {i}/{total} | Изменено: {changed}", end="\r")
    
    print(f"\nКодирование завершено. Всего: {total}, изменено: {changed}")


# ========= АЛЬТЕРНАТИВНЫЕ МЕТОДЫ ПРОВЕРКИ =========
def check_tcp_connection(host: str, port: int, timeout: int = 2) -> bool:
    """Проверяет TCP подключение к хосту"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def check_tls_handshake(host: str, port: int = 443, sni: str = None, timeout: int = 2) -> tuple:
    """
    Проверяет успешность TLS рукопожатия
    """
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        
        ssl_sock = context.wrap_socket(sock, server_hostname=sni or host)
        ssl_sock.do_handshake()
        
        version = ssl_sock.version()
        
        ssl_sock.close()
        sock.close()
        return (True, version, None)
    except Exception as e:
        return (False, None, str(e))


# ========= XRAY-ТЕСТЕР =========
class SimpleProgress:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.working_count = 0
        self.retry_count = 0
    
    def update(self, status='', working=False, retry=False):
        with self.lock:
            self.current += 1
            if working:
                self.working_count += 1
            if retry:
                self.retry_count += 1
            if self.current % 10 == 0 or self.current == self.total:
                elapsed = time.time() - self.start_time
                speed = self.current / elapsed if elapsed > 0 else 0
                print(f"\r📊 [{self.current}/{self.total}] ✅:{self.working_count} 🔄:{self.retry_count} {speed:.1f} к/с {status}", end='', flush=True)
    
    def finish(self):
        elapsed = time.time() - self.start_time
        print(f"\r✅ Готово! {self.current} конфигов за {elapsed:.1f}с ({self.current/elapsed:.1f} к/с), рабочих: {self.working_count}, повторов: {self.retry_count}")


class PortManager:
    def __init__(self, start=20000, end=25000):
        self.available = list(range(start, end + 1))
        random.shuffle(self.available)
        self.lock = threading.Lock()
    
    def get_port(self):
        with self.lock:
            if not self.available:
                return None
            return self.available.pop()
    
    def release_port(self, port):
        with self.lock:
            self.available.append(port)


# ========= ЧЁРНЫЙ СПИСОК (NOTWORKERS) — SQLite через NotworkersDB =========

class XrayTester:
    def __init__(self, input_file='url_encoded.txt', output_file='url_work.txt',
                 speed_file='url_work_speed.txt', max_workers=30,
                 top_fast_count=30, speed_test_requests=3, notworkers_db=None):
        self.input_file = input_file
        self.output_file = output_file
        self.speed_file = speed_file
        self.max_workers = max_workers
        self.top_fast_count = top_fast_count
        self.speed_test_requests = speed_test_requests
        self.strong_style_attempts = STRONG_STYLE_ATTEMPTS
        self.strong_style_timeout = STRONG_STYLE_TIMEOUT
        self.strong_max_response_time = STRONG_MAX_RESPONSE_TIME
        
        self.test_url = XRAY_TEST_URL
        self.timeout = XRAY_TIMEOUT
        self.max_retries = MAX_RETRIES
        self.retry_delay = RETRY_DELAY_BASE  # оставляем для обратной совместимости
        self.retry_delay_base = RETRY_DELAY_BASE
        self.retry_delay_multiplier = RETRY_DELAY_MULTIPLIER

        # Проверка стабильности
        self.stability_checks = STABILITY_CHECKS
        self.stability_delay = STABILITY_DELAY

        # Геолокация
        self.geo_enabled = CHECK_GEOLOCATION
        self.allowed_countries = ALLOWED_COUNTRIES
        self.geolocation_timeout = GEOLOCATION_TIMEOUT

        # Строгий режим
        self.strict_mode = STRICT_MODE
        self.strict_attempts = STRICT_ATTEMPTS
        self.strict_max_response_time = STRICT_MAX_RESPONSE_TIME
        self.strict_timeout = STRICT_TIMEOUT
        self.min_successful_requests = MIN_SUCCESSFUL_REQUESTS

        # Счётчик ошибок по URL (thread-safe) для notworkers
        self._url_last_errors: dict = {}
        self._url_errors_lock = threading.Lock()
        # NotworkersDB instance — can be passed from outside or created in test_all()
        self._db = notworkers_db
        self._db_owned = (notworkers_db is None)  # True if test_all() should create and own the DB

        self.xray_dir = Path('./xray_bin')
        if sys.platform == 'win32':
            self.xray_path = self.xray_dir / 'xray.exe'
        else:
            self.xray_path = self.xray_dir / 'xray'
        
        self.port_manager = PortManager()
        
        self.debug_file = DEBUG_FILE
        self.xray_log_file = XRAY_LOG_FILE
        
        print(f"🔍 XrayTester инициализирован")
        print(f"   📁 Входной файл: {self.input_file}")
        print(f"   📁 Рабочие адреса: {self.output_file}")
        print(f"   📁 Быстрые адреса: {self.speed_file} (топ {self.top_fast_count})")
        print(f"   ⚡ Потоков: {self.max_workers}")
        print(f"   📶 Запросов для замера скорости: {self.speed_test_requests}")
        
        self.check_xray()
        
        self.error_stats = {}

    def check_xray(self):
        """Проверяет наличие Xray"""
        if not self.xray_path.exists():
            print("⬇️ Скачиваю Xray...")
            self.download_xray()
        else:
            try:
                result = subprocess.run([str(self.xray_path), '-version'], 
                                      capture_output=True, text=True, timeout=5)
                version = result.stdout.split('\n')[0] if result.stdout else 'Unknown'
                print(f"✅ Xray готов: {version}")
            except Exception as e:
                print(f"⚠️ Xray найден, но ошибка версии: {e}")

    def download_xray(self):
        """Скачивает Xray для текущей платформы"""
        import urllib.request
        import zipfile

        self.xray_dir.mkdir(exist_ok=True)

        system = platform.system().lower()
        machine = platform.machine().lower()
        _is_arm = machine in ('arm64', 'aarch64', 'armv8', 'armv7l')

        if system == 'windows':
            archive_name = 'Xray-windows-arm64-v8a.zip' if _is_arm else 'Xray-windows-64.zip'
            xray_bin = 'xray.exe'
        elif system == 'linux':
            archive_name = 'Xray-linux-arm64-v8a.zip' if _is_arm else 'Xray-linux-64.zip'
            xray_bin = 'xray'
        elif system == 'darwin':
            archive_name = 'Xray-macos-arm64-v8a.zip' if _is_arm else 'Xray-macos-64.zip'
            xray_bin = 'xray'
        else:
            archive_name = 'Xray-linux-64.zip'
            xray_bin = 'xray'

        url = f"https://github.com/XTLS/Xray-core/releases/latest/download/{archive_name}"
        zip_path = self.xray_dir / "xray.zip"

        try:
            print(f"📥 Загрузка Xray ({archive_name})...")
            urllib.request.urlretrieve(url, zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.xray_dir)
            zip_path.unlink()
            # Make executable on Unix
            if system != 'windows':
                xray_bin_path = self.xray_dir / xray_bin
                if xray_bin_path.exists():
                    os.chmod(xray_bin_path, 0o755)
            print("✅ Xray загружен")
        except Exception as e:
            print(f"❌ Ошибка загрузки Xray: {e}")

    def parse_vless_url(self, url):
        """Парсит VLESS URL"""
        try:
            if not url.startswith('vless://'):
                return None
            
            content = url[8:]
            at_pos = content.find('@')
            if at_pos == -1:
                return None
            
            uuid = content[:at_pos]
            after_at = content[at_pos+1:]
            
            q_pos = after_at.find('?')
            if q_pos != -1:
                host_part = after_at[:q_pos]
                query = after_at[q_pos+1:]
            else:
                host_part = after_at
                query = ""
            
            hash_pos = host_part.find('#')
            if hash_pos != -1:
                host_part = host_part[:hash_pos]
            
            if ':' in host_part:
                host, port_str = host_part.split(':', 1)
                try:
                    port = int(port_str)
                except Exception:
                    port = 443
            else:
                host = host_part
                port = 443
            
            params = {}
            if query:
                for param in query.split('&'):
                    if '=' in param:
                        k, v = param.split('=', 1)
                        try:
                            v = urllib.parse.unquote(v)
                        except Exception:
                            pass
                        params[k] = v
            
            return {
                'protocol': 'vless',
                'uuid': uuid,
                'host': host,
                'port': port,
                'params': params,
                'url': url
            }
        except Exception as e:
            return None

    def parse_vmess_url(self, url: str) -> Optional[dict]:
        """Парсит VMess URL (base64-encoded JSON)"""
        try:
            if not url.lower().startswith('vmess://'):
                return None
            b64 = url[8:].split('#')[0].strip()
            padding = 4 - len(b64) % 4
            if padding != 4:
                b64 += '=' * padding
            try:
                decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
            except Exception:
                decoded = base64.urlsafe_b64decode(b64).decode('utf-8', errors='ignore')
            data = json.loads(decoded)
            return {
                'protocol': 'vmess',
                'host': str(data.get('add', '')),
                'port': int(data.get('port', 443)),
                'id': str(data.get('id', '')),
                'aid': int(data.get('aid', 0)),
                'net': str(data.get('net', 'tcp')),
                'type': str(data.get('type', 'none')),
                'host_header': str(data.get('host', '')),
                'path': str(data.get('path', '/')),
                'tls': str(data.get('tls', '')),
                'sni': str(data.get('sni', data.get('host', ''))),
                'url': url
            }
        except Exception:
            return None

    def parse_trojan_url_xray(self, url: str) -> Optional[dict]:
        """Парсит Trojan URL"""
        try:
            if not url.lower().startswith('trojan://'):
                return None
            content = url[9:]
            if '#' in content:
                content = content.split('#')[0]
            if '@' not in content:
                return None
            password, rest = content.split('@', 1)
            if '?' in rest:
                host_part, query = rest.split('?', 1)
            else:
                host_part = rest
                query = ''
            if ':' in host_part:
                host, port_str = host_part.rsplit(':', 1)
                port = int(port_str)
            else:
                host = host_part
                port = 443
            params = {}
            if query:
                for param in query.split('&'):
                    if '=' in param:
                        k, v = param.split('=', 1)
                        try:
                            v = urllib.parse.unquote(v)
                        except Exception:
                            pass
                        params[k.lower()] = v
            return {
                'protocol': 'trojan',
                'password': password,
                'host': host,
                'port': port,
                'params': params,
                'url': url
            }
        except Exception:
            return None

    def parse_ss_url_xray(self, url: str) -> Optional[dict]:
        """Парсит Shadowsocks URL"""
        try:
            if not url.lower().startswith('ss://'):
                return None
            content = url[5:]
            if '#' in content:
                content = content.split('#')[0]
            if '@' in content:
                userinfo, hostinfo = content.split('@', 1)
                try:
                    padding = 4 - len(userinfo) % 4
                    if padding != 4:
                        userinfo += '=' * padding
                    decoded = base64.b64decode(userinfo).decode('utf-8', errors='ignore')
                    method, password = decoded.split(':', 1) if ':' in decoded else ('chacha20-ietf-poly1305', decoded)
                except Exception:
                    method, password = userinfo.split(':', 1) if ':' in userinfo else ('chacha20-ietf-poly1305', userinfo)
                host_part = hostinfo.split('?', 1)[0]
                if ':' in host_part:
                    host, port_str = host_part.rsplit(':', 1)
                    port = int(port_str)
                else:
                    host = host_part
                    port = 8388
            else:
                b64 = content
                padding = 4 - len(b64) % 4
                if padding != 4:
                    b64 += '=' * padding
                decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
                if '@' not in decoded:
                    return None
                userpart, hostpart = decoded.split('@', 1)
                method, password = userpart.split(':', 1) if ':' in userpart else ('chacha20-ietf-poly1305', userpart)
                if ':' in hostpart:
                    host, port_str = hostpart.rsplit(':', 1)
                    port = int(port_str)
                else:
                    host = hostpart
                    port = 8388
            return {
                'protocol': 'ss',
                'method': method,
                'password': password,
                'host': host,
                'port': port,
                'url': url
            }
        except Exception:
            return None

    def parse_hysteria2_url_xray(self, url: str) -> Optional[dict]:
        """Парсит Hysteria2 URL"""
        try:
            url_lower = url.lower()
            if url_lower.startswith('hysteria2://'):
                content = url[12:]
            elif url_lower.startswith('hy2://'):
                content = url[6:]
            else:
                return None
            if '#' in content:
                content = content.split('#')[0]
            auth = ''
            if '@' in content:
                auth, rest = content.split('@', 1)
            else:
                rest = content
            if '?' in rest:
                host_part, query = rest.split('?', 1)
            else:
                host_part = rest
                query = ''
            if ':' in host_part:
                host, port_str = host_part.rsplit(':', 1)
                port = int(port_str)
            else:
                host = host_part
                port = 443
            params = {}
            if query:
                for param in query.split('&'):
                    if '=' in param:
                        k, v = param.split('=', 1)
                        try:
                            v = urllib.parse.unquote(v)
                        except Exception:
                            pass
                        params[k.lower()] = v
            return {
                'protocol': 'hysteria2',
                'auth': auth,
                'host': host,
                'port': port,
                'params': params,
                'url': url
            }
        except Exception:
            return None

    def _create_inbound(self, port: int) -> dict:
        """Creates a standard SOCKS5 inbound config"""
        return {
            "port": port,
            "protocol": "socks",
            "settings": {"auth": "noauth", "udp": False},
            "tag": "socks-in"
        }

    def _create_vmess_config(self, parsed: dict, port: int) -> Optional[dict]:
        """Creates Xray config for VMess"""
        try:
            net = parsed.get('net', 'tcp')
            tls = parsed.get('tls', '')
            sni = parsed.get('sni', '') or parsed.get('host_header', '') or parsed['host']
            stream = {"network": net, "security": tls}
            if tls == 'tls':
                stream["tlsSettings"] = {"serverName": sni, "allowInsecure": True}
            if net in ('ws', 'websocket'):
                stream["wsSettings"] = {
                    "path": parsed.get('path', '/'),
                    "headers": {"Host": parsed.get('host_header', '') or sni}
                }
            elif net in ('grpc', 'gun'):
                stream["grpcSettings"] = {"serviceName": parsed.get('path', ''), "multiMode": True}
            return {
                "log": {"loglevel": "error"},
                "inbounds": [self._create_inbound(port)],
                "outbounds": [{
                    "protocol": "vmess",
                    "settings": {"vnext": [{
                        "address": parsed['host'],
                        "port": parsed['port'],
                        "users": [{"id": parsed['id'], "alterId": parsed.get('aid', 0), "security": "auto"}]
                    }]},
                    "streamSettings": stream,
                    "tag": "proxy"
                }]
            }
        except Exception:
            return None

    def _create_trojan_config(self, parsed: dict, port: int) -> Optional[dict]:
        """Creates Xray config for Trojan"""
        try:
            params = parsed.get('params', {})
            security = params.get('security', 'tls') or 'tls'
            sni = params.get('sni', '') or params.get('peer', '') or parsed['host']
            net = params.get('type', 'tcp') or 'tcp'
            stream = {
                "network": net,
                "security": security,
                "tlsSettings": {"serverName": sni, "allowInsecure": True}
            }
            if net in ('ws', 'websocket'):
                stream["wsSettings"] = {
                    "path": params.get('path', '/'),
                    "headers": {"Host": params.get('host', sni)}
                }
            elif net in ('grpc', 'gun'):
                stream["grpcSettings"] = {
                    "serviceName": params.get('servicename', params.get('service', '')),
                    "multiMode": True
                }
            return {
                "log": {"loglevel": "error"},
                "inbounds": [self._create_inbound(port)],
                "outbounds": [{
                    "protocol": "trojan",
                    "settings": {"servers": [{
                        "address": parsed['host'],
                        "port": parsed['port'],
                        "password": parsed['password']
                    }]},
                    "streamSettings": stream,
                    "tag": "proxy"
                }]
            }
        except Exception:
            return None

    def _create_ss_config(self, parsed: dict, port: int) -> Optional[dict]:
        """Creates Xray config for Shadowsocks"""
        try:
            return {
                "log": {"loglevel": "error"},
                "inbounds": [self._create_inbound(port)],
                "outbounds": [{
                    "protocol": "shadowsocks",
                    "settings": {"servers": [{
                        "address": parsed['host'],
                        "port": parsed['port'],
                        "method": parsed.get('method', 'chacha20-ietf-poly1305'),
                        "password": parsed['password']
                    }]},
                    "tag": "proxy"
                }]
            }
        except Exception:
            return None

    def _check_hysteria2_alt(self, parsed: dict, url: str) -> Optional[dict]:
        """Checks Hysteria2 via TCP + TLS handshake (Xray does not support Hysteria2)"""
        host = parsed['host']
        port = parsed['port']
        sni = parsed.get('params', {}).get('sni', host)
        if not check_tcp_connection(host, port, timeout=2):
            return None
        tls_ok, _, _ = check_tls_handshake(host, port, sni, timeout=2)
        if tls_ok:
            return {'url': url, 'ping': 150, 'method': 'tls_check'}
        return None

    def create_xray_config(self, parsed, port):
        """Создает конфиг Xray для любого поддерживаемого протокола"""
        try:
            proto = parsed.get('protocol', 'vless')
            if proto == 'vmess':
                return self._create_vmess_config(parsed, port)
            elif proto == 'trojan':
                return self._create_trojan_config(parsed, port)
            elif proto == 'ss':
                return self._create_ss_config(parsed, port)
            # Default: VLESS
            params = parsed['params']
            
            flow = params.get('flow', '')
            security = params.get('security', '')
            
            config = {
                "log": {
                    "loglevel": "error"
                },
                "inbounds": [{
                    "port": port,
                    "protocol": "socks",
                    "settings": {"auth": "noauth", "udp": False},
                    "tag": "socks-in"
                }],
                "outbounds": [{
                    "protocol": "vless",
                    "settings": {
                        "vnext": [{
                            "address": parsed['host'],
                            "port": parsed['port'],
                            "users": [{
                                "id": parsed['uuid'],
                                "encryption": "none",
                                "flow": flow
                            }]
                        }]
                    },
                    "streamSettings": {
                        "network": params.get('type', 'tcp'),
                        "security": security
                    },
                    "tag": "proxy"
                }]
            }
            
            if security == 'reality':
                config["outbounds"][0]["streamSettings"]["realitySettings"] = {
                    "serverName": params.get('sni', parsed['host']),
                    "fingerprint": params.get('fp', 'chrome'),
                    "publicKey": params.get('pbk', ''),
                    "shortId": params.get('sid', ''),
                    "spiderX": params.get('spx', '/')
                }
            elif security == 'tls':
                config["outbounds"][0]["streamSettings"]["tlsSettings"] = {
                    "serverName": params.get('sni', parsed['host']),
                    "allowInsecure": True
                }
            
            if params.get('type') in ('ws', 'websocket'):
                config["outbounds"][0]["streamSettings"]["wsSettings"] = {
                    "path": params.get('path', '/'),
                    "headers": {
                        "Host": params.get('host', params.get('sni', parsed['host']))
                    }
                }
            
            if params.get('type') in ('grpc', 'gun'):
                config["outbounds"][0]["streamSettings"]["grpcSettings"] = {
                    "serviceName": params.get('servicename', params.get('service', '')),
                    "multiMode": True
                }
            
            return config
        except Exception as e:
            return None

    def _quick_parse_host_port(self, url: str) -> Optional[tuple]:
        """Быстро извлекает (host, port) из URL без создания полного dict-объекта"""
        try:
            url_lower = url.lower()
            if url_lower.startswith('vless://') or url_lower.startswith('trojan://'):
                # vless://uuid@host:port?... or trojan://pass@host:port?...
                at_pos = url.find('@')
                if at_pos == -1:
                    return None
                rest = url[at_pos + 1:].split('?')[0].split('#')[0]
                if ':' in rest:
                    host, port_str = rest.rsplit(':', 1)
                    return (host, int(port_str))
                return (rest, 443)
            elif url_lower.startswith('vmess://'):
                b64 = url[8:].split('#')[0].strip()
                padding = 4 - len(b64) % 4
                if padding != 4:
                    b64 += '=' * padding
                try:
                    decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
                except Exception:
                    decoded = base64.urlsafe_b64decode(b64).decode('utf-8', errors='ignore')
                data = json.loads(decoded)
                return (str(data.get('add', '')), int(data.get('port', 443)))
            elif url_lower.startswith('ss://'):
                content = url[5:].split('#')[0]
                if '@' in content:
                    _, hostinfo = content.split('@', 1)
                    host_part = hostinfo.split('?')[0]
                    if ':' in host_part:
                        host, port_str = host_part.rsplit(':', 1)
                        return (host, int(port_str))
                    return (host_part, 8388)
                else:
                    b64 = content
                    padding = 4 - len(b64) % 4
                    if padding != 4:
                        b64 += '=' * padding
                    decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
                    if '@' not in decoded:
                        return None
                    _, hostpart = decoded.split('@', 1)
                    if ':' in hostpart:
                        host, port_str = hostpart.rsplit(':', 1)
                        return (host, int(port_str))
                    return (hostpart, 8388)
            elif url_lower.startswith('hysteria2://') or url_lower.startswith('hy2://'):
                prefix_len = 12 if url_lower.startswith('hysteria2://') else 6
                content = url[prefix_len:].split('#')[0]
                if '@' in content:
                    _, rest = content.split('@', 1)
                else:
                    rest = content
                host_part = rest.split('?')[0]
                if ':' in host_part:
                    host, port_str = host_part.rsplit(':', 1)
                    return (host, int(port_str))
                return (host_part, 443)
        except Exception:
            pass
        return None

    def _group_by_host(self, urls: list) -> dict:
        """Группирует URL по парам (host, port). Возвращает {(host, port): [url, ...]}"""
        groups: dict = {}
        for url in urls:
            hp = self._quick_parse_host_port(url)
            if hp:
                groups.setdefault(hp, []).append(url)
        return groups

    def _needs_tls_check(self, url: str) -> bool:
        """Определяет, нужна ли TLS проверка по параметрам URL"""
        url_lower = url.lower()
        if url_lower.startswith('hysteria2://') or url_lower.startswith('hy2://'):
            return True
        if url_lower.startswith('trojan://'):
            return True
        if url_lower.startswith('vless://') or url_lower.startswith('vmess://'):
            if 'security=tls' in url_lower or 'security=reality' in url_lower:
                return True
        return False

    def _check_host_alive(self, host: str, port: int, needs_tls: bool) -> bool:
        """Проверяет доступность хоста через TCP (и TLS если нужно)"""
        if not check_tcp_connection(host, port, timeout=PREFILTER_TCP_TIMEOUT):
            return False
        if needs_tls:
            tls_ok, _, _ = check_tls_handshake(host, port, sni=host, timeout=PREFILTER_TLS_TIMEOUT)
            return tls_ok
        return True

    def pre_filter(self, urls: list) -> list:
        """
        TCP/TLS предфильтр: убирает конфиги с недоступных host:port до запуска Xray.
        Группирует по (host, port), проверяет 200 потоками (чистый I/O).
        Конфиги, прошедшие предфильтр, проверяются по полной схеме через Xray.
        """
        if not urls:
            return urls

        groups = self._group_by_host(urls)
        unique_pairs = list(groups.keys())
        total_pairs = len(unique_pairs)

        print(f"🔎 Pre-filter: {len(urls)} конфигов → {total_pairs} уникальных host:port")

        # Определяем нужность TLS для каждой пары (берём первый URL в группе)
        pair_needs_tls: dict = {}
        for hp, group_urls in groups.items():
            pair_needs_tls[hp] = any(self._needs_tls_check(u) for u in group_urls)

        alive_pairs: set = set()
        lock = threading.Lock()

        def _check_pair(hp):
            host, port = hp
            needs_tls = pair_needs_tls.get(hp, False)
            if self._check_host_alive(host, port, needs_tls):
                with lock:
                    alive_pairs.add(hp)

        workers = min(PREFILTER_WORKERS, total_pairs)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for _ in executor.map(_check_pair, unique_pairs):
                pass

        dead_pairs = total_pairs - len(alive_pairs)
        passed_urls = [url for url in urls
                       if self._quick_parse_host_port(url) in alive_pairs]
        filtered_out = len(urls) - len(passed_urls)

        print(f"🔎 Pre-filter результат: {dead_pairs} мёртвых пар host:port, "
              f"отсеяно {filtered_out} конфигов, осталось {len(passed_urls)}")

        # Записываем мёртвые конфиги в чёрный список
        if NOTWORKERS_ENABLED and self._db:
            dead_pairs_set = set(unique_pairs) - alive_pairs
            dead_urls = [url for hp in dead_pairs_set for url in groups[hp]]
            for url in dead_urls:
                self._db.add_failed(url, get_protocol_type(url), "PREFILTER_DEAD_HOST")
            if dead_urls:
                print(f"⚫ Pre-filter: {len(dead_urls)} конфигов добавлено в чёрный список")

        return passed_urls

    def _wait_for_port(self, port: int, process, timeout: float = 2.0) -> bool:
        """Ждёт открытия SOCKS5-порта с поллингом каждые 50мс вместо фиксированного sleep"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if process.poll() is not None:
                return False
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.settimeout(0.1)
                    result = sock.connect_ex(('127.0.0.1', port))
                    if result == 0:
                        return True
                finally:
                    sock.close()
            except Exception:
                pass
            time.sleep(0.05)
        return False

    def test_with_xray(self, parsed, port, attempt=1):
        """Тестирует конфиг через Xray"""
        config_file = None
        process = None
        
        try:
            config = self.create_xray_config(parsed, port)
            if not config:
                return None
            
            fd, config_file = tempfile.mkstemp(suffix='.json')
            os.close(fd)
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f)
            
            if os.name == "nt":
                creationflags = subprocess.CREATE_NO_WINDOW
            else:
                creationflags = 0
                
            process = subprocess.Popen(
                [str(self.xray_path), '-c', config_file],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=creationflags
            )
            
            self._wait_for_port(port, process, timeout=XRAY_START_WAIT)
            
            if process.poll() is not None:
                stderr = process.stderr.read() if process.stderr else "Unknown error"
                xray_error = stderr[:200]
                
                if "panic" in stderr.lower():
                    error_type = "PANIC"
                elif "fatal" in stderr.lower():
                    error_type = "FATAL"
                elif "reality" in stderr.lower():
                    error_type = "REALITY_ERROR"
                else:
                    error_type = "CRASH"
                
                return f"CRASH_{error_type}"
            
            start = time.time()
            session = requests.Session()
            session.proxies = {
                'http': f'socks5h://127.0.0.1:{port}',
                'https': f'socks5h://127.0.0.1:{port}'
            }
            session.timeout = self.timeout
            
            try:
                # STRONG_STYLE_TEST: для всех протоколов — минимум min_successful_requests из
                # strong_style_attempts/strict_attempts запросов должны быть успешными.
                # Это применяется к VLESS через strong_style_* настройки, к остальным через strict_*.
                if parsed.get('protocol') == 'vless':
                    connect_timeout = max(
                        STRONG_CONNECT_TIMEOUT_MIN,
                        min(STRONG_CONNECT_TIMEOUT_MAX, int(self.strong_style_timeout * STRONG_CONNECT_TIMEOUT_RATIO))
                    )
                    read_timeout = max(STRONG_READ_TIMEOUT_MIN, self.strong_style_timeout - connect_timeout)
                    timeout_strong = (connect_timeout, read_timeout)
                    attempts_needed = max(1, self.strong_style_attempts)
                    min_ok = self.min_successful_requests
                    pings = []

                    for i in range(attempts_needed):
                        if i > 0:
                            time.sleep(STRONG_ATTEMPT_DELAY)
                        try:
                            t = time.time()
                            r = session.get(
                                self.test_url,
                                timeout=timeout_strong,
                                allow_redirects=False,
                                verify=False
                            )
                            elapsed = time.time() - t

                            if (r.status_code in [200, 204]
                                    and len(r.content) <= GENERATE_204_MAX_CONTENT_LENGTH
                                    and (self.strong_max_response_time <= 0
                                         or elapsed <= self.strong_max_response_time)):
                                pings.append(elapsed * 1000)
                                # Adaptive timeout: если первый запрос медленный, увеличиваем таймаут
                                if i == 0 and elapsed > ADAPTIVE_TIMEOUT_THRESHOLD_SECONDS:
                                    new_read = max(read_timeout, elapsed * ADAPTIVE_TIMEOUT_MULTIPLIER)
                                    timeout_strong = (connect_timeout, new_read)
                        except Exception:
                            pass

                    if len(pings) < min_ok:
                        return "FAIL"
                    avg_ping = sum(pings) / len(pings)

                    # Проверки стабильности (общие для всех протоколов)
                    if self.stability_checks > 0:
                        for _ in range(self.stability_checks):
                            time.sleep(self.stability_delay)
                            try:
                                rs = session.get(
                                    self.test_url,
                                    timeout=timeout_strong,
                                    allow_redirects=False,
                                    verify=False
                                )
                                if rs.status_code not in [200, 204]:
                                    return "FAIL"
                            except Exception:
                                return "TIMEOUT"

                    return {'working': True, 'ping': avg_ping, 'method': 'xray'}

                # Для остальных протоколов: строгий режим (включён по умолчанию) или обычная проверка.
                if self.strict_mode:
                    # STRONG_STYLE_TEST: min_successful_requests из strict_attempts запросов должны успешно пройти
                    connect_timeout = max(
                        STRONG_CONNECT_TIMEOUT_MIN,
                        min(STRONG_CONNECT_TIMEOUT_MAX, int(self.strict_timeout * STRONG_CONNECT_TIMEOUT_RATIO))
                    )
                    read_timeout = max(STRONG_READ_TIMEOUT_MIN, self.strict_timeout - connect_timeout)
                    timeout_strict = (connect_timeout, read_timeout)
                    min_ok = self.min_successful_requests
                    strict_pings = []

                    for i in range(max(1, self.strict_attempts)):
                        if i > 0:
                            time.sleep(STRONG_ATTEMPT_DELAY)
                        try:
                            t = time.time()
                            r = session.get(
                                self.test_url,
                                timeout=timeout_strict,
                                allow_redirects=False,
                                verify=False
                            )
                            elapsed = time.time() - t

                            if (r.status_code in [200, 204]
                                    and (self.strict_max_response_time <= 0
                                         or elapsed <= self.strict_max_response_time)):
                                strict_pings.append(elapsed * 1000)
                                # Adaptive timeout: если первый запрос медленный, увеличиваем таймаут
                                if i == 0 and elapsed > ADAPTIVE_TIMEOUT_THRESHOLD_SECONDS:
                                    new_read = max(read_timeout, elapsed * ADAPTIVE_TIMEOUT_MULTIPLIER)
                                    timeout_strict = (connect_timeout, new_read)
                        except Exception:
                            pass

                    if len(strict_pings) < min_ok:
                        return "FAIL"
                    avg_ping = sum(strict_pings) / len(strict_pings)

                    # Проверки стабильности
                    if self.stability_checks > 0:
                        timeout_stab = (connect_timeout, read_timeout)
                        for _ in range(self.stability_checks):
                            time.sleep(self.stability_delay)
                            try:
                                rs = session.get(
                                    self.test_url,
                                    timeout=timeout_stab,
                                    allow_redirects=False,
                                    verify=False
                                )
                                if rs.status_code not in [200, 204]:
                                    return "FAIL"
                            except Exception:
                                return "TIMEOUT"

                    return {'working': True, 'ping': avg_ping, 'method': 'xray'}

                # Обычная проверка (не строгий режим, не VLESS)
                r = session.get(self.test_url, timeout=self.timeout, allow_redirects=False, verify=False)
                if r.status_code in [200, 204]:
                    first_ping = (time.time() - start) * 1000
                    pings = [first_ping]
                    for _ in range(self.speed_test_requests - 1):
                        try:
                            t = time.time()
                            r2 = session.get(self.test_url, timeout=self.timeout, allow_redirects=False, verify=False)
                            if r2.status_code in [200, 204]:
                                pings.append((time.time() - t) * 1000)
                        except Exception:
                            break
                    avg_ping = sum(pings) / len(pings)

                    # Проверки стабильности
                    if self.stability_checks > 0:
                        for _ in range(self.stability_checks):
                            time.sleep(self.stability_delay)
                            try:
                                rs = session.get(
                                    self.test_url,
                                    timeout=self.timeout,
                                    allow_redirects=False,
                                    verify=False
                                )
                                if rs.status_code not in [200, 204]:
                                    return "FAIL"
                            except Exception:
                                return "TIMEOUT"

                    return {'working': True, 'ping': avg_ping, 'method': 'xray'}
                return "FAIL"
            except requests.exceptions.Timeout:
                return "TIMEOUT"
            except requests.exceptions.ConnectionError as e:
                return f"CONN_ERROR"
            except Exception as e:
                return f"ERROR"
            
        except Exception as e:
            return f"EXCEPTION"
        finally:
            if process:
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        try:
                            process.wait(timeout=1.0)
                        except subprocess.TimeoutExpired:
                            pass
                except Exception:
                    pass
            if config_file and os.path.exists(config_file):
                try: 
                    os.remove(config_file)
                except Exception:
                    pass

    def check_geolocation(self, port) -> dict:
        """Проверяет геолокацию через Xray-прокси (ip-api.com)"""
        try:
            session = requests.Session()
            session.proxies = {
                'http': f'socks5h://127.0.0.1:{port}',
                'https': f'socks5h://127.0.0.1:{port}'
            }
            r = session.get(
                'https://ip-api.com/json/?fields=status,country,countryCode',
                timeout=self.geolocation_timeout,
                allow_redirects=False
            )
            if r.status_code == 200:
                data = r.json()
                if data.get('status') == 'success':
                    return {
                        'country': data.get('country', ''),
                        'country_code': data.get('countryCode', '')
                    }
        except Exception:
            pass
        return {}

    def check_alternative_methods(self, parsed, url):
        """Проверяет конфиг альтернативными методами"""
        host = parsed['host']
        port = parsed['port']
        proto = parsed.get('protocol', 'vless')

        if proto == 'trojan':
            params = parsed.get('params', {})
            security = params.get('security', 'tls') or 'tls'
            sni = params.get('sni', '') or params.get('peer', '') or host
        else:
            # VLESS
            params = parsed.get('params', {})
            security = params.get('security', '')
            sni = params.get('sni', host)

        tcp_ok = check_tcp_connection(host, port, timeout=2)

        if not tcp_ok:
            return None

        if security in ['reality', 'tls']:
            tls_ok, tls_version, _ = check_tls_handshake(host, port, sni, timeout=2)
            if tls_ok:
                return {'url': url, 'ping': 100, 'method': 'tls_check', 'security': security}

        return None

    def test_one(self, url):
        """Тестирует один конфиг (поддерживает все протоколы)"""
        proto = get_protocol_type(url)

        # Hysteria2 не поддерживается Xray — только TCP+TLS проверка
        if proto == 'hysteria2':
            parsed = self.parse_hysteria2_url_xray(url)
            if not parsed:
                return None
            return self._check_hysteria2_alt(parsed, url)

        # Выбираем парсер по протоколу
        if proto == 'vless':
            parsed = self.parse_vless_url(url)
        elif proto == 'vmess':
            parsed = self.parse_vmess_url(url)
        elif proto == 'trojan':
            parsed = self.parse_trojan_url_xray(url)
        elif proto == 'ss':
            parsed = self.parse_ss_url_xray(url)
        else:
            return None

        if not parsed:
            return None

        port = self.port_manager.get_port()
        last_error = None
        if port:
            try:
                for attempt in range(1, self.max_retries + 1):
                    result = self.test_with_xray(parsed, port, attempt)

                    if isinstance(result, dict) and result.get('working'):
                        # Проверка геолокации (если включена)
                        geo_info = {}
                        if self.geo_enabled:
                            geo_info = self.check_geolocation(port)
                            if geo_info and self.allowed_countries:
                                code = geo_info.get('country_code', '')
                                if code not in self.allowed_countries:
                                    last_error = f"GEOLOCATION_FILTERED:{code}"
                                    with self._url_errors_lock:
                                        self._url_last_errors[url] = last_error
                                    with open(self.debug_file, 'a', encoding='utf-8') as f:
                                        f.write(f"{last_error}: {url}\n")
                                    break

                        self.port_manager.release_port(port)
                        res = {'url': url, 'ping': result['ping'], 'method': 'xray'}
                        if geo_info:
                            res['country'] = geo_info.get('country', '')
                            res['country_code'] = geo_info.get('country_code', '')
                        return res

                    elif isinstance(result, str):
                        last_error = result

                        # CRASH_PANIC и CRASH_FATAL — не ретраить, сразу в блэклист
                        if result in ("CRASH_PANIC", "CRASH_FATAL"):
                            with open(self.debug_file, 'a', encoding='utf-8') as f:
                                f.write(f"{result}_{attempt}: {url}\n")
                            break

                        # Другие краши или сетевые ошибки — ретраить с экспоненциальной задержкой
                        if result.startswith("CRASH_") or result in ("TIMEOUT", "FAIL", "CONN_ERROR"):
                            if attempt < self.max_retries:
                                delay = self.retry_delay_base * (self.retry_delay_multiplier ** (attempt - 1))
                                time.sleep(delay)
                                continue
                            else:
                                with open(self.debug_file, 'a', encoding='utf-8') as f:
                                    f.write(f"{result}_{attempt}: {url}\n")

                        else:
                            with open(self.debug_file, 'a', encoding='utf-8') as f:
                                f.write(f"{result}_{attempt}: {url}\n")
                            break

                self.port_manager.release_port(port)

            except Exception:
                self.port_manager.release_port(port)

        # Сохраняем последнюю ошибку для notworkers
        if last_error:
            with self._url_errors_lock:
                self._url_last_errors[url] = last_error

        return None

    def test_all(self):
        """Тестирует все конфиги"""
        if not os.path.exists(self.input_file):
            print(f"\n❌ Нет файла {self.input_file}")
            return
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                all_urls = [line.strip() for line in f if line.strip()]
        except Exception:
            print(f"❌ Ошибка чтения файла {self.input_file}")
            return
        
        if not all_urls:
            print(f"\n📭 Нет конфигов для тестирования")
            return

        # === Фильтрация по чёрному списку (SQLite) ===
        db = None
        if NOTWORKERS_ENABLED:
            if self._db is not None:
                # Используем внешний экземпляр БД (владелец — вызывающий код)
                db = self._db
            else:
                # Создаём собственный экземпляр (test_all() закроет его сам)
                db = NotworkersDB(NOTWORKERS_DB, NOTWORKERS_FILE, ttl_days=NOTWORKERS_TTL_DAYS)
                self._db = db
            blocked_keys = db.get_blocked_keys(NOTWORKERS_MIN_FAILS)
            filtered_urls = []
            skipped_count = 0
            for url in all_urls:
                key = url.split('#')[0].strip()
                if key in blocked_keys:
                    skipped_count += 1
                else:
                    filtered_urls.append(url)
            if skipped_count:
                print(f"⚫ Пропущено (чёрный список): {skipped_count} конфигов")
            all_urls = filtered_urls
            if not all_urls:
                print(f"\n📭 Все конфиги в чёрном списке")
                if self._db_owned:
                    db.close()
                    self._db = None
                return

        # === TCP/TLS предфильтр (убирает мёртвые host:port до запуска Xray) ===
        if PREFILTER_ENABLED:
            all_urls = self.pre_filter(all_urls)
            if not all_urls:
                print(f"\n📭 Все конфиги отсеяны предфильтром")
                if NOTWORKERS_ENABLED and db and self._db_owned:
                    db.close()
                    self._db = None
                return

        print(f"\n{'='*60}")
        print(f"🔍 Тестирование {len(all_urls)} конфигов")
        print(f"⚡ Потоков: {self.max_workers}")
        print(f"⏱️ Таймаут: {self.timeout}с")
        print('='*60)
        
        # Очищаем debug файл в начале тестирования
        if os.path.exists(self.debug_file):
            os.remove(self.debug_file)
        
        working = []
        progress = SimpleProgress(len(all_urls))

        # === SIGTERM-обработчик для graceful shutdown (GitHub Actions таймаут) ===
        def _sigterm_handler(signum, frame):
            print(f"\n⚠️ Получен сигнал {signum}, завершаю...")
            if NOTWORKERS_ENABLED and hasattr(self, '_db') and self._db:
                self._db.flush()
                self._db.close()
            sys.exit(1)

        _is_main_thread = threading.current_thread() is threading.main_thread()
        old_sigterm = None
        if _is_main_thread:
            old_sigterm = signal.signal(signal.SIGTERM, _sigterm_handler)

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self.test_one, url): url for url in all_urls}
                
                checkpoint_counter = 0
                last_checkpoint_time = time.time()
                CHECKPOINT_EVERY_N = 200
                CHECKPOINT_EVERY_SEC = 60

                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        result = future.result(timeout=self.timeout + 5)
                        if result:
                            working.append(result)
                            progress.update('✅', working=True)
                            # Удаляем ожившие конфиги из чёрного списка
                            if NOTWORKERS_ENABLED and db:
                                db.remove_working(url)
                        else:
                            progress.update('❌', working=False)
                            # Обновляем чёрный список (SQLite)
                            if NOTWORKERS_ENABLED and db:
                                with self._url_errors_lock:
                                    last_error = self._url_last_errors.get(url)
                                db.add_failed(url, get_protocol_type(url), last_error)
                    except Exception as e:
                        progress.update('⚠️', working=False)

                    # === Периодический checkpoint БД ===
                    checkpoint_counter += 1
                    now = time.time()
                    if NOTWORKERS_ENABLED and db and (
                        checkpoint_counter >= CHECKPOINT_EVERY_N
                        or now - last_checkpoint_time >= CHECKPOINT_EVERY_SEC
                    ):
                        db.flush()
                        checkpoint_counter = 0
                        last_checkpoint_time = now
        finally:
            if _is_main_thread and old_sigterm is not None:
                signal.signal(signal.SIGTERM, old_sigterm)
            # Финальный flush БД перед завершением
            if NOTWORKERS_ENABLED and db:
                db.flush()
            with self._url_errors_lock:
                self._url_last_errors.clear()

        progress.finish()
        
        working.sort(key=lambda x: x['ping'])
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for w in working:
                f.write(w['url'] + '\n')
        
        # Сохраняем топ N самых быстрых адресов в отдельный файл (в стиле xraycheck)
        top_fast = working[:self.top_fast_count]
        with open(self.speed_file, 'w', encoding='utf-8') as f:
            for w in top_fast:
                f.write(w['url'] + '\n')
        
        xray_working = sum(1 for w in working if w.get('method') == 'xray')
        alt_working = len(working) - xray_working
        
        print(f"\n📊 Результаты:")
        print(f"   ✅ Работает всего: {len(working)}")
        print(f"      ├─ Через Xray: {xray_working}")
        print(f"      └─ Альтернативные методы: {alt_working}")
        print(f"   ❌ Не работает: {len(all_urls)-len(working)}")
        print(f"   🚀 Топ {len(top_fast)} быстрых сохранено в {self.speed_file}")
        
        if os.path.exists(self.debug_file):
            with open(self.debug_file, 'r', encoding='utf-8') as f:
                debug_lines = sum(1 for _ in f)
            print(f"\n🔍 Отладка: {debug_lines} ошибок в {self.debug_file}")
        
        if os.path.exists(self.xray_log_file):
            with open(self.xray_log_file, 'r', encoding='utf-8') as f:
                xray_lines = sum(1 for _ in f)
            print(f"🔍 Xray ошибки: {xray_lines} в {self.xray_log_file}")
        
        print('='*60 + '\n')

        # === Итоговая статистика чёрного списка (до закрытия БД) ===
        if NOTWORKERS_ENABLED and db:
            total = db.count()
            confirmed = db.count_confirmed(NOTWORKERS_MIN_FAILS)
            print(f"📊 Чёрный список итого: {total} записей (подтверждено: {confirmed})")
            # Закрываем БД только если test_all() сам её создал
            if self._db_owned:
                db.close()
                self._db = None
        
        return working

    def run(self):
        """Запуск тестирования"""
        self.test_all()


# ========= СТРАНА / ФЛАГ =========
def country_code_to_flag(code: str) -> str:
    """Конвертирует ISO-3166 двухбуквенный код страны в Unicode-эмодзи флага"""
    try:
        code = code.strip().upper()
        if len(code) != 2 or not code.isalpha():
            return "🏳"
        return chr(0x1F1E6 + ord(code[0]) - ord('A')) + chr(0x1F1E6 + ord(code[1]) - ord('A'))
    except Exception:
        return "🏳"


def extract_host_ip(vless_url: str) -> Optional[str]:
    """Извлекает хост или IP-адрес из URL любого поддерживаемого протокола"""
    try:
        url_no_frag = vless_url.split('#', 1)[0]
        proto = get_protocol_type(url_no_frag)

        if proto == 'vless':
            content = url_no_frag[len("vless://"):]
            at_pos = content.find('@')
            if at_pos == -1:
                return None
            after_at = content[at_pos + 1:]
            q_pos = after_at.find('?')
            if q_pos != -1:
                after_at = after_at[:q_pos]
            if after_at.startswith('['):
                bracket_end = after_at.find(']')
                if bracket_end != -1:
                    return after_at[1:bracket_end]
            if ':' in after_at:
                return after_at.rsplit(':', 1)[0]
            return after_at or None

        elif proto == 'vmess':
            try:
                b64 = url_no_frag[8:].strip()
                padding = 4 - len(b64) % 4
                if padding != 4:
                    b64 += '=' * padding
                decoded = base64.b64decode(b64).decode('utf-8', errors='ignore')
                data = json.loads(decoded)
                return data.get('add') or None
            except Exception:
                return None

        elif proto in ('trojan', 'ss', 'hysteria2'):
            if proto == 'hysteria2':
                content = url_no_frag[len('hysteria2://'):] if url_no_frag.lower().startswith('hysteria2://') else url_no_frag[len('hy2://'):]
            elif proto == 'trojan':
                content = url_no_frag[len('trojan://'):]
            else:
                content = url_no_frag[len('ss://'):]
            after_at = content.split('@', 1)[1] if '@' in content else content
            host_part = after_at.split('?', 1)[0]
            if ':' in host_part:
                return host_part.rsplit(':', 1)[0]
            return host_part or None

    except Exception:
        return None


def get_countries_batch(ips: list) -> dict:
    """
    Запрашивает страны для списка IP через ip-api.com batch endpoint.
    Возвращает словарь {ip: {'country': ..., 'countryCode': ...}}.
    """
    result = {}
    if not ips:
        return result
    # ip-api.com batch API принимает до 100 адресов за раз
    batch_size = 100
    try:
        for i in range(0, len(ips), batch_size):
            batch = ips[i:i + batch_size]
            payload = [{"query": ip, "fields": "query,country,countryCode,status"} for ip in batch]
            response = requests.post(
                "http://ip-api.com/batch",
                json=payload,
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                for entry in data:
                    query_ip = entry.get("query", "")
                    if entry.get("status") == "success":
                        result[query_ip] = {
                            "country": entry.get("country", "Unknown"),
                            "countryCode": entry.get("countryCode", ""),
                        }
                    else:
                        result[query_ip] = {"country": "Unknown", "countryCode": ""}
            else:
                for ip in batch:
                    result[ip] = {"country": "Unknown", "countryCode": ""}
    except Exception as e:
        print(f"⚠️ Ошибка запроса к ip-api.com: {e}")
        for ip in ips:
            if ip not in result:
                result[ip] = {"country": "Unknown", "countryCode": ""}
    return result


async def rename_working_configs(notworkers_db=None):
    """
    Переписывает url_work.txt и url_work_speed.txt, присваивая каждому конфигу
    имя вида «🇷🇺 Russia 1», «🇷🇺 Russia 2», … (флаг + страна + порядковый номер).
    Конфиги НЕ из России и Unknown отсеиваются в чёрный список NOTWORKERS.
    """
    ALLOWED_GEO = {"russia", "unknown"}
    print("\n=== Переименование рабочих конфигов по стране ===")

    for filepath in (WORK_FILE, SPEED_FILE):
        if not os.path.exists(filepath):
            continue

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                lines = [line.rstrip("\n") for line in f]
        except Exception as e:
            print(f"⚠️ Не удалось прочитать {filepath}: {e}")
            continue

        urls = [l for l in lines if l.strip()]
        if not urls:
            continue

        # Собираем уникальные хосты/IP
        hosts = []
        for url in urls:
            host = extract_host_ip(url.split('#', 1)[0])
            hosts.append(host or "")

        unique_ips = list({h for h in hosts if h})
        ip_info = get_countries_batch(unique_ips) if unique_ips else {}

        # Счётчики по странам
        country_counters: dict = {}
        renamed = []
        geo_blocked = 0

        for url, host in zip(urls, hosts):
            info = ip_info.get(host, {"country": "Unknown", "countryCode": ""})
            country = info.get("country", "Unknown") or "Unknown"
            code = info.get("countryCode", "") or ""

            # Фильтрация по стране — оставляем только Russia и Unknown
            if country.lower() not in ALLOWED_GEO:
                geo_blocked += 1
                if NOTWORKERS_ENABLED and notworkers_db:
                    notworkers_db.add_failed(url, get_protocol_type(url), f"GEO_BLOCKED:{country}")
                continue

            flag = country_code_to_flag(code) if code else "🏳"

            country_counters[country] = country_counters.get(country, 0) + 1
            num = country_counters[country]

            base = url.split('#', 1)[0]
            new_url = f"{base}#{flag} {country} {num}"
            renamed.append(new_url)

        if geo_blocked:
            print(f"🌍 {filepath}: отсеяно по геолокации: {geo_blocked} конфигов (не RU/Unknown)")

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                for line in renamed:
                    f.write(line + "\n")
            print(f"✅ {filepath}: переименовано {len(renamed)} конфигов")
        except Exception as e:
            print(f"⚠️ Не удалось записать {filepath}: {e}")


# ========= ОСНОВНОЙ ЦИКЛ =========
async def main_cycle():
    global cycle_counter
    cycle_counter += 1
    print(f"\n=== Новый цикл #{cycle_counter} ===")

    # Ранняя инициализация NotworkersDB — гарантирует создание файла .db
    # и запуск TTL-очистки в начале каждого цикла
    notworkers_db = None
    if NOTWORKERS_ENABLED:
        notworkers_db = NotworkersDB(NOTWORKERS_DB, NOTWORKERS_FILE, ttl_days=NOTWORKERS_TTL_DAYS)

    try:
        await _run_cycle_logic(notworkers_db)
    finally:
        if notworkers_db is not None:
            notworkers_db.close()


async def _run_cycle_logic(notworkers_db):
    
    if cycle_counter % CYCLES_BEFORE_DEBUG_CLEAN == 0:
        if os.path.exists(DEBUG_FILE):
            os.remove(DEBUG_FILE)
            print(f"🧹 Очищен {DEBUG_FILE} после {cycle_counter} циклов")
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    if not os.path.exists(SOURCES_FILE):
        print(f"❌ Нет файла {SOURCES_FILE}")
        return

    try:
        with open(SOURCES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    except Exception:
        print(f"❌ Ошибка чтения {SOURCES_FILE}")
        return

    if not urls:
        print("⚠️ Нет URL для скачивания")
        return

    print(f"📥 Загружаю {len(urls)} источников...")
    
    sem = asyncio.Semaphore(THREADS_DOWNLOAD)
    output_lock = asyncio.Lock()
    stats = {"processed": 0, "found": 0}

    async with aiohttp.ClientSession() as session:
        tasks = [process_url(session, url, sem, output_lock, stats) for url in urls]
        await asyncio.gather(*tasks)

    print(f"\n✅ Скачивание завершено. Найдено конфигов: {stats['found']}")
    await log(f"Скачивание завершено. Найдено конфигов: {stats['found']}")

    if stats['found'] > 0:
        await clean_vless()
        await filter_vless()
        await rename_configs()
        await encode_all_configs()
    else:
        print("⏭️ Нет новых конфигов из источников, проверяем ранее найденные адреса")

    # Объединяем новые конфиги с ранее работавшими адресами из url_work.txt и url_work_speed.txt
    urls_to_test = []
    url_keys_seen = set()

    files_to_merge = []
    if stats['found'] > 0 and os.path.exists(ENCODED_FILE):
        files_to_merge.append(ENCODED_FILE)
    files_to_merge.extend([WORK_FILE, SPEED_FILE])

    for file_to_read in files_to_merge:
        if os.path.exists(file_to_read):
            try:
                with open(file_to_read, 'r', encoding='utf-8') as f:
                    for line in f:
                        url = line.strip()
                        if url:
                            url_key = url.split('#', 1)[0]
                            if url_key not in url_keys_seen:
                                url_keys_seen.add(url_key)
                                urls_to_test.append(url)
            except Exception as e:
                print(f"⚠️ Ошибка чтения {file_to_read}: {e}")

    if not urls_to_test:
        print("⏭️ Нет конфигов для проверки")
        return

    # Единственный экземпляр notworkers_db передаётся в XrayTester для фильтрации
    # по чёрному списку и записи ошибок — одно соединение с БД на весь цикл.

    print(f"\n📋 Итого для проверки: {len(urls_to_test)} конфигов")
    with open(COMBINED_FILE, 'w', encoding='utf-8') as f:
        for url in urls_to_test:
            f.write(url + '\n')

    print("\n=== Запуск Xray-проверки ===")
    tester = XrayTester(
        input_file=COMBINED_FILE,
        output_file=WORK_FILE,
        speed_file=SPEED_FILE,
        max_workers=XRAY_MAX_WORKERS,
        top_fast_count=TOP_FAST_COUNT,
        speed_test_requests=SPEED_TEST_REQUESTS,
        notworkers_db=notworkers_db,
    )
    
    tester.run()

    await rename_working_configs(notworkers_db)


async def run_forever():
    print("\n🔄 Запуск бесконечного цикла...")
    while True:
        try:
            cycle_start = time.time()
            await main_cycle()
            cycle_time = time.time() - cycle_start
            print(f"✅ Цикл завершен за {cycle_time:.1f}с")
            print(f"⏳ Ожидание {CYCLE_DELAY//3600} час до следующего цикла...")
            await asyncio.sleep(CYCLE_DELAY)
        except KeyboardInterrupt:
            print("\n👋 Остановка по запросу пользователя")
            break
        except Exception as e:
            print(f"\n❌ Ошибка в цикле: {e}")
            print(f"⏳ Ожидание 60 секунд перед перезапуском...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    parser_args = argparse.ArgumentParser(description="Парсер VLESS-конфигов #РКП")
    parser_args.add_argument(
        "--once",
        action="store_true",
        help="Выполнить один цикл и завершить (используется в GitHub Actions)"
    )
    args = parser_args.parse_args()

    try:
        if args.once:
            asyncio.run(main_cycle())
        else:
            asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("\n👋 Программа остановлена")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
