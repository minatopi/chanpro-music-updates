import os
import sys
import time
import json
import hashlib
import threading
from pathlib import Path
from datetime import datetime, timezone

import psycopg
import paho.mqtt.client as mqtt


# ============================================================
# 設定
# ============================================================

MQTT_HOST = "broker.hivemq.com"
MQTT_PORT = 8883

# Webブラウザ側で使用している
# wss://broker.hivemq.com:8884/mqtt
# の通常MQTT/TLS版
MQTT_USE_TLS = True

MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

DATABASE_URL = os.environ["DATABASE_URL"]

# GitHubリポジトリ内のバックアップ先
BACKUP_DIR = Path("mqtt_backup")

# 実際の音楽チャンク
# 例:
# chanpro/audio/v1/1789279320135-tniz6ykt/00000000
MQTT_ROOT = "chanpro/audio/v1"

# 1チャンク
CHUNK_TOPIC_FORMAT = "{prefix}/{index:08d}"

# MQTT待機時間
CONNECT_TIMEOUT = 30
RECEIVE_TIMEOUT = 15

# MQTT再送信間隔
PUBLISH_INTERVAL = 0.03

# QoS
MQTT_QOS = 1

# バックアップ用manifest
MANIFEST_FILE = BACKUP_DIR / "manifest.json"


# ============================================================
# 共通
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def sha256(data: bytes):
    return hashlib.sha256(data).hexdigest()


def safe_name(value: str):
    """
    MQTT IDをファイル名に安全にする。
    """
    return "".join(
        c if c.isalnum() or c in "-_"
        else "_"
        for c in value
    )


def topic_to_backup_path(topic: str):
    """
    MQTT topic
        chanpro/audio/v1/<music_id>/00000000

    を

        mqtt_backup/<music_id>/00000000.bin

    に変換。
    """

    prefix = MQTT_ROOT.rstrip("/") + "/"

    if not topic.startswith(prefix):
        return None

    rest = topic[len(prefix):]

    parts = rest.split("/")

    if len(parts) != 2:
        return None

    music_id = safe_name(parts[0])
    chunk_name = parts[1]

    if not chunk_name.isdigit():
        return None

    return BACKUP_DIR / music_id / f"{chunk_name}.bin"


def chunk_index_from_topic(topic: str):
    prefix = MQTT_ROOT.rstrip("/") + "/"

    if not topic.startswith(prefix):
        return None, None

    rest = topic[len(prefix):]
    parts = rest.split("/")

    if len(parts) != 2:
        return None, None

    music_id = parts[0]

    if not parts[1].isdigit():
        return None, None

    return music_id, int(parts[1])


# ============================================================
# Supabase / PostgreSQL
# ============================================================

def load_music_files():
    """
    music_filesから有効な音楽データを取得。
    """

    sql = """
        SELECT
            id,
            user_id,
            title,
            artist,
            file_name,
            mime,
            original_size,
            compressed_size,
            chunks,
            compression,
            mqtt_prefix,
            thumbnail_url,
            has_thumbnail,
            created_at,
            updated_at,
            deleted
        FROM public.music_files
        WHERE deleted = FALSE
        ORDER BY created_at ASC
    """

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    columns = [
        "id",
        "user_id",
        "title",
        "artist",
        "file_name",
        "mime",
        "original_size",
        "compressed_size",
        "chunks",
        "compression",
        "mqtt_prefix",
        "thumbnail_url",
        "has_thumbnail",
        "created_at",
        "updated_at",
        "deleted",
    ]

    result = []

    for row in rows:
        item = dict(zip(columns, row))

        if item["mqtt_prefix"]:
            result.append(item)

    return result


# ============================================================
# Manifest
# ============================================================

def load_manifest():

    if not MANIFEST_FILE.exists():
        return {
            "version": 1,
            "updated_at": None,
            "files": {}
        }

    try:
        with MANIFEST_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)

    except Exception as e:
        print("manifest読み込み失敗:", e)

        return {
            "version": 1,
            "updated_at": None,
            "files": {}
        }


def save_manifest(manifest):

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    manifest["updated_at"] = now_iso()

    temp = MANIFEST_FILE.with_suffix(".tmp")

    with temp.open("w", encoding="utf-8") as f:
        json.dump(
            manifest,
            f,
            ensure_ascii=False,
            indent=2
        )

    temp.replace(MANIFEST_FILE)


# ============================================================
# MQTT
# ============================================================

class MQTTManager:

    def __init__(self):

        self.client = None

        self.connected_event = threading.Event()

        self.messages = {}

        self.lock = threading.Lock()

        self.error = None

    # --------------------------------------------------------
    # 接続
    # --------------------------------------------------------

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):

        print("MQTT接続:", reason_code)

        if reason_code == 0:
            self.connected_event.set()
        else:
            self.error = f"MQTT connect error: {reason_code}"

    # --------------------------------------------------------
    # 切断
    # --------------------------------------------------------

    def on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags=None,
        reason_code=None,
        properties=None
    ):

        print("MQTT切断:", reason_code)

    # --------------------------------------------------------
    # メッセージ
    # --------------------------------------------------------

    def on_message(
        self,
        client,
        userdata,
        msg
    ):

        music_id, index = chunk_index_from_topic(msg.topic)

        if music_id is None:
            return

        with self.lock:

            if music_id not in self.messages:
                self.messages[music_id] = {}

            # retainedだけではなく通常メッセージも受信可能
            self.messages[music_id][index] = bytes(msg.payload)

    # --------------------------------------------------------
    # 開始
    # --------------------------------------------------------

    def start(self):

        client_id = (
            "chanpro-github-refresh-"
            + str(os.getpid())
            + "-"
            + str(int(time.time()))
        )

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5
        )

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        if MQTT_USERNAME:
            self.client.username_pw_set(
                MQTT_USERNAME,
                MQTT_PASSWORD
            )

        if MQTT_USE_TLS:
            self.client.tls_set()

        print(
            f"MQTT接続開始: {MQTT_HOST}:{MQTT_PORT}"
        )

        self.client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=60
        )

        self.client.loop_start()

        if not self.connected_event.wait(
            CONNECT_TIMEOUT
        ):
            raise RuntimeError(
                "MQTT接続タイムアウト"
            )

    # --------------------------------------------------------
    # 停止
    # --------------------------------------------------------

    def stop(self):

        if self.client:

            try:
                self.client.disconnect()
            except Exception:
                pass

            try:
                self.client.loop_stop()
            except Exception:
                pass

    # --------------------------------------------------------
    # 購読
    # --------------------------------------------------------

    def subscribe(self, topic):

        print("SUBSCRIBE:", topic)

        result, mid = self.client.subscribe(
            topic,
            qos=MQTT_QOS
        )

        if result != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(
                f"subscribe failed: {result}"
            )

    # --------------------------------------------------------
    # 受信データクリア
    # --------------------------------------------------------

    def clear_music(self, music_id):

        with self.lock:
            self.messages[music_id] = {}

    # --------------------------------------------------------
    # 現在受信したデータ
    # --------------------------------------------------------

    def get_chunks(self, music_id):

        with self.lock:
            return dict(
                self.messages.get(
                    music_id,
                    {}
                )
            )

    # --------------------------------------------------------
    # Retain再送信
    # --------------------------------------------------------

    def publish_retain(
        self,
        topic,
        payload
    ):

        info = self.client.publish(
            topic,
            payload=payload,
            qos=MQTT_QOS,
            retain=True
        )

        if info.rc != mqtt.MQTT_ERR_SUCCESS:

            raise RuntimeError(
                f"publish failed: {info.rc}"
            )

        info.wait_for_publish(
            timeout=30
        )


# ============================================================
# GitHubバックアップ
# ============================================================

def backup_chunk(
    music_id,
    index,
    payload
):

    music_dir = (
        BACKUP_DIR /
        safe_name(music_id)
    )

    music_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    path = music_dir / f"{index:08d}.bin"

    # 同じデータなら書き換えない
    if path.exists():

        try:
            old = path.read_bytes()

            if old == payload:
                return False

        except Exception:
            pass

    temp = path.with_suffix(".tmp")

    temp.write_bytes(payload)

    temp.replace(path)

    return True


def read_backup_chunk(
    music_id,
    index
):

    path = (
        BACKUP_DIR /
        safe_name(music_id) /
        f"{index:08d}.bin"
    )

    if not path.exists():
        return None

    return path.read_bytes()


def backup_has_all_chunks(
    music_id,
    chunks
):

    for index in range(chunks):

        if read_backup_chunk(
            music_id,
            index
        ) is None:

            return False

    return True


# ============================================================
# MQTTから既存Retainを取得
# ============================================================

def receive_retained_music(
    mqtt_manager,
    music
):

    music_id = str(music["id"])

    prefix = str(
        music["mqtt_prefix"]
    ).rstrip("/")

    expected = int(
        music["chunks"] or 0
    )

    if expected <= 0:
        print(
            f"[SKIP] {music_id}: chunks=0"
        )
        return {}

    mqtt_manager.clear_music(
        music_id
    )

    topic = prefix + "/#"

    mqtt_manager.subscribe(
        topic
    )

    print(
        f"[RECEIVE] {music_id} "
        f"{expected} chunks"
    )

    deadline = (
        time.time()
        + RECEIVE_TIMEOUT
    )

    while time.time() < deadline:

        chunks = mqtt_manager.get_chunks(
            music_id
        )

        if len(chunks) >= expected:

            break

        time.sleep(0.2)

    chunks = mqtt_manager.get_chunks(
        music_id
    )

    print(
        f"[RECEIVE DONE] {music_id}: "
        f"{len(chunks)}/{expected}"
    )

    return chunks


# ============================================================
# 1曲処理
# ============================================================

def process_music(
    mqtt_manager,
    music,
    manifest
):

    music_id = str(music["id"])

    expected = int(
        music["chunks"] or 0
    )

    prefix = str(
        music["mqtt_prefix"]
    ).rstrip("/")

    if expected <= 0:
        return {
            "status": "skip",
            "music_id": music_id
        }

    print()
    print("=" * 70)
    print(
        f"音楽: {music.get('title', '')}"
    )
    print(
        f"ID: {music_id}"
    )
    print(
        f"chunks: {expected}"
    )
    print(
        f"prefix: {prefix}"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # まずGitHubバックアップの状態を確認
    # --------------------------------------------------------

    backup_complete = backup_has_all_chunks(
        music_id,
        expected
    )

    # --------------------------------------------------------
    # MQTT Retainを取得
    # --------------------------------------------------------

    mqtt_chunks = receive_retained_music(
        mqtt_manager,
        music
    )

    restored = 0
    downloaded = 0
    changed = 0
    missing = []

    # --------------------------------------------------------
    # 各チャンクを確認
    # --------------------------------------------------------

    for index in range(expected):

        topic = CHUNK_TOPIC_FORMAT.format(
            prefix=prefix,
            index=index
        )

        mqtt_payload = mqtt_chunks.get(
            index
        )

        backup_payload = read_backup_chunk(
            music_id,
            index
        )

        # ====================================================
        # MQTTに存在
        # ====================================================

        if mqtt_payload is not None:

            downloaded += 1

            # GitHubバックアップを更新
            if backup_chunk(
                music_id,
                index,
                mqtt_payload
            ):
                changed += 1

            # ------------------------------------------------
            # MQTT自体も再Publish
            # ------------------------------------------------

            try:

                mqtt_manager.publish_retain(
                    topic,
                    mqtt_payload
                )

                print(
                    f"[REFRESH] "
                    f"{music_id} "
                    f"{index + 1}/{expected}"
                )

            except Exception as e:

                print(
                    f"[ERROR] publish "
                    f"{topic}: {e}"
                )

                raise

        # ====================================================
        # MQTTにない
        # ====================================================

        else:

            # GitHubバックアップがあれば復元
            if backup_payload is not None:

                print(
                    f"[RESTORE] "
                    f"{music_id} "
                    f"{index + 1}/{expected}"
                )

                mqtt_manager.publish_retain(
                    topic,
                    backup_payload
                )

                restored += 1

            else:

                missing.append(
                    index
                )

                print(
                    f"[MISSING] "
                    f"{music_id} "
                    f"chunk={index}"
                )

        time.sleep(
            PUBLISH_INTERVAL
        )

    # --------------------------------------------------------
    # Manifest更新
    # --------------------------------------------------------

    manifest["files"][music_id] = {
        "id": music_id,
        "title": music.get("title", ""),
        "artist": music.get("artist", ""),
        "file_name": music.get("file_name", ""),
        "mime": music.get("mime", ""),
        "original_size": music.get(
            "original_size",
            0
        ),
        "compressed_size": music.get(
            "compressed_size",
            0
        ),
        "chunks": expected,
        "compression": music.get(
            "compression",
            "gzip"
        ),
        "mqtt_prefix": prefix,
        "backup_complete": (
            len(missing) == 0
        ),
        "last_refresh": now_iso(),
        "missing_chunks": missing,
        "sha256_chunks": {},
    }

    # --------------------------------------------------------
    # チャンクSHA256
    # --------------------------------------------------------

    for index in range(expected):

        data = read_backup_chunk(
            music_id,
            index
        )

        if data is not None:

            manifest["files"][
                music_id
            ]["sha256_chunks"][
                str(index)
            ] = sha256(data)

    # --------------------------------------------------------
    # 結果
    # --------------------------------------------------------

    if missing:

        return {
            "status": "missing",
            "music_id": music_id,
            "downloaded": downloaded,
            "restored": restored,
            "missing": missing,
            "backup_changed": changed,
        }

    return {
        "status": "ok",
        "music_id": music_id,
        "downloaded": downloaded,
        "restored": restored,
        "backup_changed": changed,
    }


# ============================================================
# メイン
# ============================================================

def main():

    print()
    print("============================================================")
    print(" ChanPro MQTT Retain Refresh")
    print("============================================================")
    print(
        "開始:",
        now_iso()
    )
    print()

    # --------------------------------------------------------
    # バックアップディレクトリ
    # --------------------------------------------------------

    BACKUP_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    manifest = load_manifest()

    # --------------------------------------------------------
    # Supabase
    # --------------------------------------------------------

    print("Supabaseから音楽一覧を取得しています...")

    music_files = load_music_files()

    print(
        f"対象音楽: {len(music_files)}"
    )

    if not music_files:

        print(
            "対象となるmusic_filesがありません。"
        )

        save_manifest(manifest)

        return 0

    # --------------------------------------------------------
    # MQTT
    # --------------------------------------------------------

    mqtt_manager = MQTTManager()

    try:

        mqtt_manager.start()

        results = []

        # ----------------------------------------------------
        # 全音楽
        # ----------------------------------------------------

        for music in music_files:

            try:

                result = process_music(
                    mqtt_manager,
                    music,
                    manifest
                )

                results.append(result)

            except Exception as e:

                music_id = str(
                    music["id"]
                )

                print()
                print(
                    f"[FATAL MUSIC ERROR] "
                    f"{music_id}: {e}"
                )

                results.append({
                    "status": "error",
                    "music_id": music_id,
                    "error": str(e)
                })

        # ----------------------------------------------------
        # Manifest
        # ----------------------------------------------------

        save_manifest(
            manifest
        )

        # ----------------------------------------------------
        # 結果
        # ----------------------------------------------------

        print()
        print("============================================================")
        print("結果")
        print("============================================================")

        ok = 0
        restored = 0
        missing = 0
        errors = 0

        for result in results:

            status = result["status"]

            if status == "ok":
                ok += 1

            elif status == "missing":
                missing += 1

            elif status == "error":
                errors += 1

            print(
                result
            )

            if result.get("restored", 0):
                restored += result["restored"]

        print()
        print(
            f"正常: {ok}"
        )
        print(
            f"復元チャンク: {restored}"
        )
        print(
            f"不足ファイル: {missing}"
        )
        print(
            f"エラー: {errors}"
        )

        # ----------------------------------------------------
        # MQTTに完全なデータがない場合は失敗扱い
        # ----------------------------------------------------

        if missing > 0:

            print()
            print(
                "警告: GitHubバックアップにも存在しない"
                "MQTTチャンクがあります。"
            )

            print(
                "このチャンクはMQTTだけからは復元できません。"
            )

            return 1

        if errors > 0:
            return 1

        print()
        print(
            "MQTT Retain更新完了"
        )

        return 0

    finally:

        mqtt_manager.stop()


if __name__ == "__main__":

    try:
        sys.exit(
            main()
        )

    except KeyboardInterrupt:

        print(
            "中断されました。"
        )

        sys.exit(1)

    except Exception as e:

        print()
        print(
            "============================================================"
        )
        print(
            "致命的エラー"
        )
        print(
            "============================================================"
        )

        print(e)

        sys.exit(1)
