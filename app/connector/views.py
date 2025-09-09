import requests

from ninja import Router, Query

router = Router()


# Создаем SSL-контекст из PFX
client_key = "connector/secrets/client.key"
client_crt = "connector/secrets/client.crt"
ca_crt = "connector/secrets/ca.crt"
url = '192.168.0.107'

@router.get('/test-connector', response={200: str, 400: str})
def test_connector(request):
    # Подключаемся
    response = requests.get(
        url,
        cert=(client_crt, client_key),  # клиентский сертификат + ключ
        verify=ca_crt  # корневой сертификат (или True, если публичный CA)
    )
    return 200, response.text()


@router.get('/get-info')
def get_list(request):
    pass