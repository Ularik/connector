import ssl
import httpx

from ninja import Router, Query

router = Router()


# Создаем SSL-контекст из PFX
pfx_file = "connector/secrets/client-wunat.pfx"
pfx_password = b"pass123!"

context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

# Загружаем pfx
context.load_pkcs12(open(pfx_file, "rb").read(), pfx_password)


@router.get('/test-connector')
def test_connector(request):
    # Подключаемся
    with httpx.Client(verify=context) as client:
        r = client.get("https://test.kg")
        print(r.status_code, r.text)


@router.get('/get-info')
def get_list(request):
    pass