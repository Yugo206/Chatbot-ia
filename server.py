import psycopg2
from urllib.parse import urlparse
from flask import Flask, request, jsonify, render_template, Response, session, stream_with_context
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
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL manquant. Configure la base PostgreSQL.")

    conn = psycopg2.connect(
        dsn=DATABASE_URL,
        sslmode="require",
        connect_timeout=20
    )
    conn.autocommit = True
    return conn

app = Flask(__name__)
app.secret_key = "dev-secret-key"

def generate_csrf_token():
    if "csrf_token" not in session:
        session["csrf_token"] = os.urandom(16).hex()
    return session["csrf_token"]

@app.before_request
def csrf_protect():
    if request.method in ["GET", "HEAD", "OPTIONS"]:
        return

    # allow login without CSRF only if no session exists yet
    if request.path == "/login":
        return
    if request.path == "/stream":
        return

    token = session.get("csrf_token")
    header_token = request.headers.get("X-CSRF-Token")

    if not token:
        return jsonify({"error": "CSRF manquant"}), 403

    if token != header_token:
        return jsonify({"error": "CSRF invalide"}), 403

app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 7  # 7 days

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=True # mettre True en HTTPS (prod)
)

# CORS correct
CORS(app, supports_credentials=True)

# ---------------- QUEUE ----------------

queue = []
active_user = None
queue_lock = threading.Lock()

# ---------------- CONTEXT MANAGEMENT ----------------
user_contexts = {}

# -------- SECURITY --------
login_attempts = {}  # {ip: [timestamps]}
MAX_ATTEMPTS = 5
BLOCK_TIME = 60  # seconds

MAX_CONTEXT_MESSAGES = 10
MAX_INPUT_TOKENS = 150
MAX_OUTPUT_TOKENS = 1000

def trim_context(context):
    return context[-MAX_CONTEXT_MESSAGES:]

# ---------------- ROUTES ----------------
@app.route("/")
def home():
    print(client.base_url)
    return render_template("index.html")

# -------- HEALTH CHECK --------
@app.route("/health", methods=["GET"])
def health():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        conn.close()

        return jsonify({
            "ready": True
        })

    except Exception as e:
        print("[HEALTH ERROR]", str(e))

        return jsonify({
            "ready": False,
            "message": "Réveil de la base de données..."
        }), 503

# -------- LOGIN --------
@app.route("/login", methods=["POST"])
def login():
    ip = request.remote_addr
    now = time.time()

    # Clean old attempts
    attempts = login_attempts.get(ip, [])
    attempts = [t for t in attempts if now - t < BLOCK_TIME]
    login_attempts[ip] = attempts

    if len(attempts) >= MAX_ATTEMPTS:
        return jsonify({"error": "Trop de tentatives. Réessaie plus tard."}), 429

    data = request.get_json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()

    if not username or not password:
        return jsonify({"error": "Champs manquants"}), 400

    try:
        conn = get_db()
    except Exception:
        return jsonify({
            "error": "Le serveur démarre encore. Réessayez dans quelques secondes."
        }), 503

    with conn:
        cur = conn.cursor()
        cur.execute("SELECT mdp FROM users WHERE username = %s", (username,))
        row = cur.fetchone()

        if not row or row[0] != password:
            attempts.append(now)
            login_attempts[ip] = attempts
            return jsonify({"error": "Identifiants invalides"}), 401

    login_attempts[ip] = []  # reset after success
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
        "username": session["user"],
        "csrf_token": generate_csrf_token()
    })


# -------- LOGOUT --------
@app.route("/logout", methods=["POST", "GET"])
def logout():
    session.clear()
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

    try:
        conn = get_db()
    except Exception:
        return jsonify({
            "error": "Le serveur se réveille encore. Réessayez dans quelques secondes."
        }), 503

    with conn:
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

        yield f"data: {json.dumps({'type':'start'})}\n\n"

        # -------- FILE D’ATTENTE --------
        with queue_lock:
            if username not in queue:
                queue.append(username)

        while True:
            with queue_lock:
                if username not in queue:
                    return

                if username not in queue:
                    position = 0
                else:
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
                    {"role": "system", "content": "Tu es un assistant utile, clair et fiable. Tu réponds simplement et de façon pédagogique. Donne des explications faciles à comprendre avec des exemples si utile. Si tu n’es pas sûr d’une information, dis-le honnêtement. Évite les réponses inutilement longues. À la fin, tu peux proposer brièvement une aide complémentaire pertinente."},
                    *context
                ],
                temperature=0.2,
                max_tokens=MAX_OUTPUT_TOKENS,
                stream=True
            )

            for chunk in stream:
                try:
                    token = chunk.choices[0].delta.content
                except Exception:
                    token = None

                if token:
                    full_response += token
                    yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"

            # save assistant response
            context.append({"role": "assistant", "content": full_response})
            context = trim_context(context)

            user_contexts[username] = context

            # decrement messages
            conn = get_db()
            try:
                cur = conn.cursor()
                cur.execute("UPDATE users SET message_restant = message_restant - 1 WHERE username = %s", (username,))
            finally:
                conn.close()

        except Exception as e:
            yield f"data: {json.dumps({'type':'error','content':str(e)})}\n\n"

        # -------- FIN --------
        with queue_lock:
            if queue and queue[0] == username:
                queue.pop(0)
            active_user = None

        yield f"data: {json.dumps({'type':'done'})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6767, threaded=True)