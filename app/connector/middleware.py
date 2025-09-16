import jwt
from ninja.security import HttpBearer
from datetime import datetime

# Подписи проверяются публичным ключом (сертификатом CA или конкретного клиента)
PUBLIC_KEY = open("secrets/.pem").read()

class JWTAuth(HttpBearer):
    def authenticate(self, request, token):
        try:
            payload = jwt.decode(
                token,
                PUBLIC_KEY,
                algorithms=["RS256"],   # или ES256, зависит от ключей
                options={"require": ["exp", "iat"]}
            )
            return payload  # Можно вернуть user_id и т.п.
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None

auth = JWTAuth()
