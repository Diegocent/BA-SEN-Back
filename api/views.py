from rest_framework import viewsets, generics
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from django.db.models import Sum, Count, Q, F
from .models import HechosAsistenciaHumanitaria
from .serializers import HechosAsistenciaHumanitariaSerializer, TotalAyudasSerializer
import logging

# Configurar logging para ver las consultas
# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger('django.db.backends').setLevel(logging.DEBUG)

# Configuración de paginación
class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'per_page'
    max_page_size = 100

class AsistenciaDetalladaViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Vista que devuelve el detalle de los registros, incluyendo la ubicación,
    el evento y las cantidades de ayuda. Soporta filtros y paginación.
    """
    queryset = HechosAsistenciaHumanitaria.objects.all().select_related('id_fecha', 'id_ubicacion', 'id_evento')
    serializer_class = HechosAsistenciaHumanitariaSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        queryset = super().get_queryset()
        
        # Filtros específicos
        departamento = self.request.query_params.get('departamento')
        distrito = self.request.query_params.get('distrito')
        evento = self.request.query_params.get('evento')
        localidad = self.request.query_params.get('localidad')
        
        if departamento:
            queryset = queryset.filter(id_ubicacion__departamento__icontains=departamento)
        if distrito:
            queryset = queryset.filter(id_ubicacion__distrito__icontains=distrito)
        if evento:
            queryset = queryset.filter(id_evento__evento__icontains=evento)
        if localidad:
            queryset = queryset.filter(id_ubicacion__localidad__icontains=localidad)
        
        # Búsqueda general
        input_busqueda = self.request.query_params.get('inputBusqueda')
        if input_busqueda:
            queryset = queryset.filter(
                Q(id_ubicacion__departamento__icontains=input_busqueda) |
                Q(id_ubicacion__distrito__icontains=input_busqueda) |
                Q(id_ubicacion__localidad__icontains=input_busqueda) |
                Q(id_evento__evento__icontains=input_busqueda)
            )
            
        return queryset.order_by('-id_fecha__fecha')

class AsistenciaAnualAPIView(generics.ListAPIView):
    """
    Endpoint para ver la asistencia humanitaria total por año.
    """
    serializer_class = TotalAyudasSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return HechosAsistenciaHumanitaria.objects.values('id_fecha__anio').annotate(
            kit_sentencia=Sum('kit_sentencia'),
            kit_evento=Sum('kit_evento'),
            chapas=Sum('chapa_fibrocemento_cantidad') + Sum('chapa_zinc_cantidad')
        ).order_by('id_fecha__anio')

class AsistenciaMensualAPIView(generics.ListAPIView):
    """
    Endpoint para ver la asistencia humanitaria total por mes.
    """
    serializer_class = TotalAyudasSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        queryset = HechosAsistenciaHumanitaria.objects.values('id_fecha__anio', 'id_fecha__mes', 'id_fecha__nombre_mes').annotate(
            kit_sentencia=Sum('kit_sentencia'),
            kit_evento=Sum('kit_evento'),
            chapas=Sum('chapa_fibrocemento_cantidad') + Sum('chapa_zinc_cantidad')
        ).order_by('id_fecha__anio', 'id_fecha__mes')
        
        anio_param = self.request.query_params.get('anio')
        if anio_param:
            queryset = queryset.filter(id_fecha__anio=anio_param)

        return queryset
    
class AsistenciaPorUbicacionAPIView(generics.ListAPIView):
    """
    Endpoint para ver la asistencia humanitaria por departamento y distrito.
    """
    serializer_class = TotalAyudasSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        queryset = HechosAsistenciaHumanitaria.objects.values('id_ubicacion__departamento', 'id_ubicacion__distrito').annotate(
            kit_sentencia=Sum('kit_sentencia'),
            kit_evento=Sum('kit_evento'),
            chapas=Sum('chapa_fibrocemento_cantidad') + Sum('chapa_zinc_cantidad')
        ).order_by('id_ubicacion__departamento', 'id_ubicacion__distrito')
        
        departamento_param = self.request.query_params.get('departamento')
        if departamento_param:
            queryset = queryset.filter(id_ubicacion__departamento__icontains=departamento_param)
        
        distrito_param = self.request.query_params.get('distrito')
        if distrito_param:
            queryset = queryset.filter(id_ubicacion__distrito__icontains=distrito_param)

        return queryset

class AsistenciaPorEventoAPIView(generics.ListAPIView):
    """
    Endpoint para ver la asistencia humanitaria por tipo de evento.
    """
    serializer_class = TotalAyudasSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        return HechosAsistenciaHumanitaria.objects.values(
            tipoEvento=F('id_evento__evento')
        ).annotate(
            numeroOcurrencias=Count('id_evento'),
            kit_sentencia=Sum('kit_sentencia'),
            kit_evento=Sum('kit_evento'),
            chapas=Sum('chapa_fibrocemento_cantidad') + Sum('chapa_zinc_cantidad')
        ).order_by('-numeroOcurrencias')

class EventosPorDepartamentoAPIView(generics.ListAPIView):
    """
    Endpoint para ver las cantidades de ayuda para eventos específicos en cada departamento.
    """
    serializer_class = TotalAyudasSerializer
    pagination_class = StandardResultsSetPagination
    
    def get_queryset(self):
        queryset = HechosAsistenciaHumanitaria.objects.values('id_ubicacion__departamento', 'id_evento__evento').annotate(
            kit_sentencia=Sum('kit_sentencia'),
            kit_evento=Sum('kit_evento'),
            chapas=Sum('chapa_fibrocemento_cantidad') + Sum('chapa_zinc_cantidad')
        ).order_by('id_ubicacion__departamento', 'id_evento__evento')
        
        departamento_param = self.request.query_params.get('departamento')
        if departamento_param:
            queryset = queryset.filter(id_ubicacion__departamento__icontains=departamento_param)
            
        input_busqueda = self.request.query_params.get('inputBusqueda')
        if input_busqueda:
            queryset = queryset.filter(
                Q(id_ubicacion__departamento__icontains=input_busqueda) |
                Q(id_evento__evento__icontains=input_busqueda)
            )

        return queryset
