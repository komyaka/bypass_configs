import sqlite3
import os
import json
import threading
from datetime import datetime, timezone


class NotworkersDB:
    def __init__(self, db_path="configs/notworkers.db", json_path="configs/notworkers.json", ttl_days=7):
        """Создаёт/открывает БД, создаёт таблицу и индексы, выполняет автомиграцию из JSON"""
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.db_path = db_path
        self.ttl_days = ttl_days
        self._lock = threading.Lock()
        self._pending_writes = 0
        self._batch_size = 50
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

        # Автомиграция: если JSON существует, а БД пуста — импортируем
        if os.path.exists(json_path):
            count = self.conn.execute("SELECT COUNT(*) FROM notworkers").fetchone()[0]
            if count == 0:
                self._migrate_from_json(json_path)

        # TTL-очистка при старте
        self.cleanup_ttl()

    def _create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS notworkers (
                url_key       TEXT PRIMARY KEY,
                raw           TEXT,
                protocol      TEXT,
                first_seen    TEXT NOT NULL,
                last_seen     TEXT NOT NULL,
                fail_count    INTEGER DEFAULT 1,
                fail_streak   INTEGER DEFAULT 1,
                last_error    TEXT
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON notworkers(last_seen)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fail_streak ON notworkers(fail_streak)")
        self.conn.commit()

    def _migrate_from_json(self, json_path):
        """Одноразовая миграция из JSON"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            entries = data.get('entries', data) if isinstance(data, dict) and 'entries' in data else data

            migrated = 0
            for key, info in entries.items():
                self.conn.execute("""
                    INSERT OR IGNORE INTO notworkers
                    (url_key, raw, protocol, first_seen, last_seen, fail_count, fail_streak, last_error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    key,
                    info.get('raw', key),
                    info.get('protocol', 'unknown'),
                    info.get('first_seen', ''),
                    info.get('last_seen', ''),
                    info.get('fail_count', 1),
                    info.get('fail_streak', info.get('fail_count', 1)),
                    info.get('last_error')
                ))
                migrated += 1

            self.conn.commit()
            print(f"📦 Миграция JSON → SQLite: {migrated} записей импортировано")

            # Переименовываем JSON в бэкап (не удаляем!)
            backup = json_path + '.migrated'
            os.rename(json_path, backup)
            print(f"📁 JSON сохранён как бэкап: {backup}")

            # Также переименовываем текстовый файл если есть
            text_path = os.path.splitext(json_path)[0]  # configs/notworkers
            if os.path.exists(text_path):
                os.rename(text_path, text_path + '.migrated')

        except Exception as e:
            print(f"⚠️ Ошибка миграции JSON → SQLite: {e}")

    def add_failed(self, url, protocol='unknown', error=None):
        """Добавляет/обновляет запись о провале"""
        key = url.split('#')[0].strip()
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        with self._lock:
            self.conn.execute("""
                INSERT INTO notworkers (url_key, raw, protocol, first_seen, last_seen, fail_count, fail_streak, last_error)
                VALUES (?, ?, ?, ?, ?, 1, 1, ?)
                ON CONFLICT(url_key) DO UPDATE SET
                    last_seen = ?,
                    fail_count = fail_count + 1,
                    fail_streak = fail_streak + 1,
                    last_error = COALESCE(?, last_error)
            """, (key, url, protocol, now_str, now_str, error, now_str, error))
            self._pending_writes += 1
            if self._pending_writes >= self._batch_size:
                self.conn.commit()
                self._pending_writes = 0

    def remove_working(self, url):
        """Удаляет ожившие конфиги (но не GEO-заблокированные — они навсегда)"""
        key = url.split('#')[0].strip()
        with self._lock:
            self.conn.execute(
                "DELETE FROM notworkers WHERE url_key = ? "
                "AND (last_error IS NULL OR last_error NOT LIKE 'GEO_BLOCKED:%')",
                (key,)
            )
            self.conn.commit()

    def is_blocked(self, url, min_fails=2):
        """Проверка без загрузки всей БД"""
        key = url.split('#')[0].strip()
        row = self.conn.execute(
            "SELECT 1 FROM notworkers WHERE url_key = ? AND fail_streak >= ?",
            (key, min_fails)
        ).fetchone()
        return row is not None

    def get_blocked_keys(self, min_fails=2):
        """Возвращает set заблокированных ключей"""
        rows = self.conn.execute(
            "SELECT url_key FROM notworkers WHERE fail_streak >= ?", (min_fails,)
        ).fetchall()
        return {row[0] for row in rows}

    def cleanup_ttl(self, ttl_days=None):
        """Очистка по TTL — GEO-заблокированные записи не удаляются (они навсегда)"""
        days = ttl_days or self.ttl_days
        cursor = self.conn.execute(
            "DELETE FROM notworkers WHERE last_seen < datetime('now', ?) "
            "AND (last_error IS NULL OR last_error NOT LIKE 'GEO_BLOCKED:%')",
            (f'-{days} days',)
        )
        self.conn.commit()
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"🗑️ Очищено из чёрного списка по TTL ({days}д): {deleted} записей")
        return deleted

    def count(self):
        return self.conn.execute("SELECT COUNT(*) FROM notworkers").fetchone()[0]

    def count_confirmed(self, min_fails=2):
        return self.conn.execute(
            "SELECT COUNT(*) FROM notworkers WHERE fail_streak >= ?", (min_fails,)
        ).fetchone()[0]

    def count_geo_blocked(self):
        """Возвращает количество записей, заблокированных навсегда по геолокации"""
        return self.conn.execute(
            "SELECT COUNT(*) FROM notworkers WHERE last_error LIKE 'GEO_BLOCKED:%'"
        ).fetchone()[0]

    def checkpoint(self):
        """Сбрасывает WAL-журнал в основной файл БД (периодическое сохранение)"""
        try:
            with self._lock:
                self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception as e:
            print(f"⚠️ Ошибка checkpoint: {e}")

    def flush(self):
        """Принудительный commit + checkpoint"""
        try:
            with self._lock:
                if self._pending_writes > 0:
                    self.conn.commit()
                    self._pending_writes = 0
                self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception as e:
            print(f"⚠️ Ошибка flush: {e}")

    def close(self):
        try:
            with self._lock:
                if self._pending_writes > 0:
                    self.conn.commit()
                    self._pending_writes = 0
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.conn.close()
        except Exception:
            pass
        # Удаляем WAL/SHM файлы чтобы не мешать git операциям
        for suffix in ('-wal', '-shm'):
            try:
                wal_path = self.db_path + suffix
                if os.path.exists(wal_path):
                    os.remove(wal_path)
            except Exception:
                pass

    def export_text(self, filepath):
        """Экспортирует ключи в текстовый файл для совместимости"""
        try:
            rows = self.conn.execute("SELECT url_key FROM notworkers").fetchall()
            with open(filepath, 'w', encoding='utf-8') as f:
                for row in rows:
                    f.write(row[0] + '\n')
        except Exception as e:
            print(f"⚠️ Ошибка экспорта: {e}")
