from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import os
from project.settings_local import SECRETS_PATH
from django.core.management.base import BaseCommand


# создаем папку с приватным и публичным ключом
if not os.path.exists(SECRETS_PATH):
    os.mkdir(SECRETS_PATH)


class Command(BaseCommand):
    help = "Моя кастомная команда"

    def create_key(self):
        # Генерация приватного ключа RSA 2048 бит
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Сохраняем приватный ключ
        with open(f"{SECRETS_PATH}/private.pem", "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))

        # Извлекаем публичный ключ
        public_key = private_key.public_key()

        # Сохраняем публичный ключ
        with open(f"{SECRETS_PATH}/public.pem", "wb") as f:
            f.write(public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ))

        self.stdout.write(self.style.SUCCESS("RSA ключи успешно сгенерированы → private.pem и public.pem"))

    def add_arguments(self, parser):
        parser.add_argument("--name", type=str, help="Имя пользователя")

    def handle(self, *args, **options):
        name = options.get("name")
        if name:
            self.stdout.write(self.style.SUCCESS(f"Hello, {name}!"))
        else:
            self.create_key()
