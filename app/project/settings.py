from pathlib import Path
import os
from corsheaders.defaults import default_headers
from datetime import timedelta

BASE_DIR = Path(__file__).resolve().parent.parent

from .settings_local import *


if DEV:
    # Dev
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': DB_NAME_DEV,
            'USER': DB_USER_DEV,
            'PASSWORD': DB_PASS_DEV,
            'HOST': DB_HOST_DEV,
            'PORT': DB_PORT_DEV,
            'TEST': {
                'NAME': DB_NAME_DEV_TEST,
            },
        }
    }

    SIMPLE_JWT = {
        'UPDATE_LAST_LOGIN': True,
        'ROTATE_REFRESH_TOKENS': True,
        'ACCESS_TOKEN_LIFETIME': timedelta(days=1),  # minutes=2
        'REFRESH_TOKEN_LIFETIME': timedelta(days=30),
    }


else:
    # Prod
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': DB_NAME,
            'USER': DB_USER,
            'PASSWORD': DB_PASS,
            'HOST': DB_HOST,
            'PORT': DB_PORT,
            'TEST': {
                'NAME': DB_NAME_TEST,
            },
        }
    }

    SIMPLE_JWT = {
        'UPDATE_LAST_LOGIN': True,
        'ROTATE_REFRESH_TOKENS': True,
        'ACCESS_TOKEN_LIFETIME': timedelta(days=1),
        'REFRESH_TOKEN_LIFETIME': timedelta(days=30),
    }



INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    'rest_framework',
    'rest_framework.authtoken',
    'rest_framework_simplejwt',

    'ninja_jwt',
    'ninja_extra',
    'celery',
    'corsheaders',
    'db_logger',
    'connector',
    # 'livereload', # автоматическая перезагрузка страницы при изменении кода, замедляет работу, на проде выключать livereload.middleware.LiveReloadScript

    'user'
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'livereload.middleware.LiveReloadScript',
]


REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.SessionAuthentication',
        'rest_framework.authentication.TokenAuthentication',
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ],
    'DEFAULT_THROTTLE_CLASSES': [
        'rest_framework.throttling.AnonRateThrottle',
        'rest_framework.throttling.UserRateThrottle'
    ],
    'DEFAULT_THROTTLE_RATES': {
        'anon': '10/second',
        'user': '20/second'
    },
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 100,
    'DEFAULT_FILTER_BACKENDS': ['django_filters.rest_framework.DjangoFilterBackend'],
}

CORS_ORIGIN_ALLOW_ALL = True

CORS_ALLOW_HEADERS = (
    *default_headers,
    "device",
    "jwtToken",
    "JWToken",
)

ASGI_APPLICATION = 'project.asgi.application'

CELERY_BROKER_URL = "redis://redis:6379"
CELERY_RESULT_BACKEND = "redis://redis:6379"
CELERY_TASK_TIME_LIMIT = 3600

ROOT_URLCONF = 'project.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR,'templates')],
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

WSGI_APPLICATION = 'project.wsgi.application'


AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
        'OPTIONS': {
            'min_length': 6,
        },
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


LANGUAGE_CODE = 'ru'

USE_I18N = True

USE_L10N = False

USE_TZ = False

DATE_FORMAT = 'd.m.Y'
TIME_FORMAT = 'H:i'
DATETIME_FORMAT = 'd.m.Y, H:i'

DATETIME_INPUT_FORMATS = [
    '%d.%m.%Y, %H:%M:%S',
    '%d.%m.%Y, %H:%M',
]


STATIC_URL = '/static/'

if not DEBUG:
    STATIC_ROOT = os.path.join(BASE_DIR, 'static')
else:
    STATICFILES_DIRS = [
        os.path.join(BASE_DIR, "static")
    ]

MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, '..', 'media')

if not os.path.exists(MEDIA_ROOT):
    os.makedirs(MEDIA_ROOT)


DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


AUTH_USER_MODEL = 'user.CustomUser'


LOGS_DIR = os.path.join(BASE_DIR, '../logs/')

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)




LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(name)-12s %(levelname)-8s %(message)s'
        },
        'file': {
            'format': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
        },
        'db_log': {
            'format': '%(name)-12s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
        'file_django': {
            'class': 'logging.FileHandler',  # logging.handlers.RotatingFileHandler - если нужно пересоздавать
            'formatter': 'file',
            'filename': LOGS_DIR + 'django.log',
        },
        'db_log': {
            'level': 'DEBUG',
            'formatter': 'db_log',
            'class': 'db_logger.db_log_handler.DatabaseLogHandler'
        },
        'file_api': {
            'class': 'logging.FileHandler',
            'formatter': 'file',
            'filename': LOGS_DIR + 'api.log',
        },
    },
    'loggers': {
        'django': {
            'level': 'WARNING',
            'handlers': ['console', 'file_django']
        },
        'API': {
            'level': 'INFO',
            'handlers': ['console', 'db_log', 'file_api'],
        },
        'connector': {
            'level': 'INFO',
            'handlers': ['console', 'db_log', 'file_django'],
        }
    },
}

DJANGO_DB_LOGGER_ENABLE_FORMATTER = True