import traceback

from django.contrib import admin
from django.http import JsonResponse
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

from ninja_jwt.controller import NinjaJWTDefaultController
from ninja_extra import NinjaExtraAPI

from django.contrib.auth.decorators import login_required

import logging

logger = logging.getLogger('API')


api = NinjaExtraAPI(
   title="API",
   description="Документация API",
    version="1.0",
)
# https://swagger.io/docs/open-source-tools/swagger-ui/usage/configuration/

api.register_controllers(NinjaJWTDefaultController)

@api.exception_handler(Exception)
def custom_500_handler(request, exc):
    logger.exception(f'{request.build_absolute_uri()}')
    error_details = {
        "message": "Internal Server Error",
        "details": str(exc),
        "traceback": traceback.format_exc()
    }
    return JsonResponse(error_details, status=500)

from main.views import router as main_router
from connector.views import router as conn_router

api.add_router('main/', main_router, tags=["Главная"])
api.add_router('conn/', conn_router, tags=["Коннектор"])


urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', api.urls, name='api'),

    path('', include('main.urls'), name='index'),
    path('', include('user.urls'), name='accounts'),
]

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.DEBUG:
    urlpatterns += staticfiles_urlpatterns()