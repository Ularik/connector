import requests

from ninja import Router, Query

router = Router()


@router.get('/get-info', response={200: str, 400: str})
def get_list(request):
    return 200, 'good connect'