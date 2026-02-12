import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Jjjb3509')
DB_NAME = os.getenv('DB_NAME', 'db_pnatrans')

# String de conexão
DATABASE_URL = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# --- CONFIGURAÇÕES DA APLICAÇÃO ---
APP_TITLE = "Monitoramento PNATRANS"
APP_ICON = "📊"
APP_LAYOUT = "wide"

# --- CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANILHAS_DIR = os.path.join(BASE_DIR, 'Planilhas')
SCRIPTS_DIR = os.path.join(BASE_DIR, 'scripts')
CONFIG_DIR = os.path.join(BASE_DIR, 'config')

# --- CACHE TTL (em segundos) ---
CACHE_TTL = 300

# --- DEBUG ---
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
