from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    AsistenciaDetalladaAPIView,
    AsistenciaAnualAPIView,
    AsistenciaMensualAPIView,
    AsistenciaPorUbicacionAPIView,
    AsistenciaPorDepartamentoAPIView,
    AsistenciaPorEventoAPIView,
    EventosPorDepartamentoAPIView,
    total_asistencia,
    EventosPorLocalidadAPIView,
    AsistenciasPorAnioDepartamentoAPIView,
    TendenciaMensualAsistenciasAPIView,
    DistribucionMensualDetalladaAPIView,
    DistribucionAnualProductoAPIView,
    AsistenciasPorEventoAPIView,
    ComposicionAyudasPorEventoAPIView,
    OcurrenciasEventoAnualAPIView,
    IncendiosAnualesPorDepartamentoAPIView,
    ResumenGeneralAPIView,
    ResumenPorDepartamentoAPIView,
)



urlpatterns = [
    path('detallada/', AsistenciaDetalladaAPIView.as_view(), name='asistencia-detallada'),
    # Rutas para las APIViews
    path('anual/', AsistenciaAnualAPIView.as_view(), name='asistencia-anual'),
    path('mensual/', AsistenciaMensualAPIView.as_view(), name='asistencia-mensual'),
    path('por-ubicacion/', AsistenciaPorUbicacionAPIView.as_view(), name='asistencia-por-ubicacion'),
    path('por-departamento/', AsistenciaPorDepartamentoAPIView.as_view(), name='asistencia-por-departamento'),
    path('por-evento/', AsistenciaPorEventoAPIView.as_view(), name='asistencia-por-evento'),
    path('eventos-por-departamento/', EventosPorDepartamentoAPIView.as_view(), name='eventos-por-departamento'),
    # Endpoints adicionales
    path('total/', total_asistencia, name='total-asistencia'),
    path('eventos-por-localidad/', EventosPorLocalidadAPIView.as_view(), name='eventos-por-localidad'),
    path('asistencias-por-anio-departamento/', AsistenciasPorAnioDepartamentoAPIView.as_view(), name='asistencias-por-anio-departamento'),
    path('tendencia-mensual-asistencias/', TendenciaMensualAsistenciasAPIView.as_view(), name='tendencia-mensual-asistencias'),
    path('distribucion-mensual-detallada/', DistribucionMensualDetalladaAPIView.as_view(), name='distribucion-mensual-detallada'),
    path('distribucion-anual-producto/', DistribucionAnualProductoAPIView.as_view(), name='distribucion-anual-producto'),
    path('asistencias-por-evento/', AsistenciasPorEventoAPIView.as_view(), name='asistencias-por-evento'),
    path('composicion-ayudas-por-evento/', ComposicionAyudasPorEventoAPIView.as_view(), name='composicion-ayudas-por-evento'),
    path('ocurrencias-evento-anual/', OcurrenciasEventoAnualAPIView.as_view(), name='ocurrencias-evento-anual'),
    path('incendios-anuales-por-departamento/', IncendiosAnualesPorDepartamentoAPIView.as_view(), name='incendios-anuales-por-departamento'),
    path('resumen-general/', ResumenGeneralAPIView.as_view(), name='resumen-general'),
    path('resumen-por-departamento/', ResumenPorDepartamentoAPIView.as_view(), name='resumen-por-departamento'),
]
