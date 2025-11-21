import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'tu_clave_secreta_aqui'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# ALLOWED_HOSTS: leer desde la variable de entorno DJANGO_ALLOWED_HOSTS
_hosts = os.environ.get(
			'DJANGO_ALLOWED_HOSTS',  
			'161.35.53.140,127.0.0.1,sen.yvagacore.com,sen-api.yvagacore.com,192.168.100.236'
)
ALLOWED_HOSTS = [h.strip() for h in _hosts.split(',') if h.strip()]


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'corsheaders',  # <--- Agregado para CORS
    'api', # Asegúrate de que tu aplicación 'api' esté aquí
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',  # <--- Agregado para CORS (debe ir arriba)
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]
# --- Configuración de CORS ---
CORS_ALLOWED_ORIGINS = [
    "https://sen.yvagacore.com",
    "https://sen-api.yvagacore.com",
    "http://192.168.100.101",
    "http://192.168.100.101:3000",
    "http://192.168.100.101:5173",
    "http://161.35.53.140:3227",
    "http://161.35.53.140",
]

CSRF_TRUSTED_ORIGINS = [
    "https://sen.yvagacore.com",
    "https://sen-api.yvagacore.com",
]

# Permitir también credenciales (cookies, auth, etc.)
CORS_ALLOW_CREDENTIALS = True

# Si querés permitir headers personalizados:
CORS_ALLOW_HEADERS = [
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "dnt",
    "origin",
    "user-agent",
    "x-csrftoken",
    "x-requested-with",
]

ROOT_URLCONF = 'businessAnalitycsBack.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'businessAnalitycsBack.wsgi.application'


# --- CONFIGURACIÓN DE BASES DE DATOS ---
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'data_warehouse',
        'USER': 'neondb_owner',
        'PASSWORD': 'npg_4jZsgShV0xAd',
        'HOST': 'ep-gentle-meadow-acqg6ndn-pooler.sa-east-1.aws.neon.tech',
        'PORT': '5432',
        'OPTIONS': {
            'sslmode': 'require',
            'channel_binding': 'require',
        },
        'CONN_MAX_AGE': 60,
    }
}

# Password validation
# https://docs.djangoproject.com/en/4.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.0/topics/i18n/

LANGUAGE_CODE = 'es-py'

TIME_ZONE = 'America/Asuncion'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.0/howto/static-files/

STATIC_URL = '/static/'
