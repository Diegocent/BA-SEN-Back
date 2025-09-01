from django.urls import path
from . import views

urlpatterns = [
    # URLs para los endpoints de la API
    path('asistencias/anual/', views.api_asistencias_anual, name='api_asistencias_anual'),
    path('asistencias/mensual/', views.api_asistencias_mensual, name='api_asistencias_mensual'),
    path('asistencias/ubicacion/', views.api_asistencias_por_ubicacion, name='api_asistencias_por_ubicacion'),
    path('asistencias/evento/', views.api_asistencias_por_evento, name='api_asistencias_por_evento'),
    path('asistencias/evento_por_departamento/', views.api_eventos_por_departamento, name='api_eventos_por_departamento'),
    path('asistencias/detallados/', views.api_registros_detallados, name='api_registros_detallados'),
]