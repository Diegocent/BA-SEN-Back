from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    AsistenciaDetalladaViewSet,
    AsistenciaAnualAPIView,
    AsistenciaMensualAPIView,
    AsistenciaPorUbicacionAPIView,
    AsistenciaPorEventoAPIView,
    EventosPorDepartamentoAPIView,
)

# El router se usa para los ViewSets
router = DefaultRouter()
router.register(r'detallada', AsistenciaDetalladaViewSet, basename='asistencia-detallada')

urlpatterns = [
    # Rutas para el ViewSet (con paginación y filtros automáticos)
    path('', include(router.urls)),

    # Rutas para las APIViews
    path('anual/', AsistenciaAnualAPIView.as_view(), name='asistencia-anual'),
    path('mensual/', AsistenciaMensualAPIView.as_view(), name='asistencia-mensual'),
    path('por-ubicacion/', AsistenciaPorUbicacionAPIView.as_view(), name='asistencia-por-ubicacion'),
    path('por-evento/', AsistenciaPorEventoAPIView.as_view(), name='asistencia-por-evento'),
    path('eventos-por-departamento/', EventosPorDepartamentoAPIView.as_view(), name='eventos-por-departamento'),
]
