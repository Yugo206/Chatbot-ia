import sqlite3
import psycopg2
from urllib.parse import urlparse
from flask import Flask, request, jsonify, render_template, Response, session
from flask_cors import CORS
import threading
import time
import json
import os
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

# ---------------- CONFIG ----------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY manquant. Ajoute ta clé API dans les variables d'environnement.")
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL_NAME = "gpt-4o-mini"

# ---------------- DB CONFIG ---------------
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db():
    if DATABASE_URL:
        # PostgreSQL (Render)
        url = urlparse(DATABASE_URL)
        conn = psycopg2.connect(
            dbname=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        return conn
    else:
        # SQLite (local)
        DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")
        conn = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

app = Flask(__name__)
app.secret_key = "dev-secret-key"
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 7  # 7 days

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=False  # mettre True en HTTPS (prod)
)

# CORS correct
CORS(app, supports_credentials=True)

# ---------------- QUEUE ----------------

queue = []
active_user = None
queue_lock = threading.Lock()

# ---------------- CONTEXT MANAGEMENT ----------------
user_contexts = {}
MAX_CONTEXT_MESSAGES = 8
MAX_INPUT_TOKENS = 100
MAX_OUTPUT_TOKENS = 500

def trim_context(context):
    return context[-MAX_CONTEXT_MESSAGES:]

# ---------------- ROUTES ----------------
@app.route("/")
def home():
    return render_template("index.html")

# -------- LOGIN --------
@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()

    if not username or not password:
        return jsonify({"error": "Champs manquants"}), 400

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT mdp FROM users WHERE username = %s", (username,))
        row = cur.fetchone()

        if not row or row[0] != password:
            return jsonify({"error": "Identifiants invalides"}), 401

    session["user"] = username
    session.permanent = True
    return jsonify({"success": True, "username": username})


# -------- SESSION CHECK --------
@app.route("/me", methods=["GET"])
def me():
    if "user" not in session:
        return jsonify({"logged": False})

    return jsonify({
        "logged": True,
        "username": session["user"]
    })


# -------- LOGOUT --------
@app.route("/logout", methods=["POST"])
def logout():
    session.pop("user", None)
    return jsonify({"success": True})


# -------- STREAM --------
@app.route("/stream", methods=["POST"])
def stream():
    # Vérification de session
    if "user" not in session:
        return jsonify({"error": "Non authentifié"}), 403

    username = session["user"]
    data = request.get_json()
    message = (data.get("message") or "").strip()

    # ---------------- INPUT TOKEN LIMIT (approx) ----------------
    # 100 tokens ≈ ~400-500 characters (approximation)
    MAX_INPUT_CHARS = 450

    if len(message) > MAX_INPUT_CHARS:
        message = message[:MAX_INPUT_CHARS]

    if not message:
        return jsonify({"error": "Message vide"}), 400

    # ---------- Vérification DB avant file d'attente ----------
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT mdp, message_restant FROM users WHERE username = %s", (username,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Utilisateur introuvable"}), 401

        password, messages_restantes = row
        if messages_restantes <= 0:
            return jsonify({"error": "Messages restants épuisés"}), 402

    # À partir d’ici, on sait que l’utilisateur peut continuer
    # ----------- File d’attente et streaming ----------
    def generate():
        global active_user


        # -------- FILE D’ATTENTE --------
        with queue_lock:
            if username not in queue:
                queue.append(username)

        while True:
            with queue_lock:
                position = queue.index(username)
                if position == 0 and active_user is None:
                    active_user = username
                    break

            yield f"data: {json.dumps({'type':'queue','position':position})}\n\n"
            time.sleep(1)
        # -------- STREAM OPENAI --------
        try:
            context = user_contexts.get(username, [])

            # ensure user message is safely limited before storing in context
            safe_message = message[:MAX_INPUT_CHARS]
            context.append({"role": "user", "content": safe_message})
            context = trim_context(context)

            full_response = ""

            stream = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": "Tu es un assistant expert en développement. Donne des réponses détaillées, pédagogiques, avec des exemples de code et des explications claires étape par étape."},
                    *context
                ],
                temperature=0.2,
                max_tokens=MAX_OUTPUT_TOKENS,
                stream=True
            )

            for chunk in stream:
                if chunk.choices[0].delta.content:
                    token = chunk.choices[0].delta.content
                    full_response += token
                    yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"

            # save assistant response
            context.append({"role": "assistant", "content": full_response})
            context = trim_context(context)

            user_contexts[username] = context

            # decrement messages
            try:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute("UPDATE users SET message_restant = message_restant - 1 WHERE username = %s", (username,))
                    conn.commit()
            except Exception:
                pass

        except Exception as e:
            yield f"data: {json.dumps({'type':'error','content':str(e)})}\n\n"

        # -------- FIN --------
        with queue_lock:
            if queue and queue[0] == username:
                queue.pop(0)
            active_user = None

        yield f"data: {json.dumps({'type':'done'})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run()