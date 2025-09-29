from rest_framework import serializers
from .models import HechosAsistenciaHumanitaria, DimFecha, DimUbicacion, DimEvento

class DimFechaSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimFecha
        fields = ['fecha', 'anio', 'mes', 'dia_del_mes', 'nombre_mes']

class DimUbicacionSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimUbicacion
        fields = ['departamento', 'distrito', 'localidad', 'orden']

class DimEventoSerializer(serializers.ModelSerializer):
    class Meta:
        model = DimEvento
        fields = ['evento']

class HechosAsistenciaHumanitariaSerializer(serializers.ModelSerializer):
    # Relacionar los campos de la tabla de hechos con los serializadores de las dimensiones
    fecha = serializers.DateField(source='id_fecha.fecha', read_only=True)
    departamento = serializers.CharField(source='id_ubicacion.departamento', read_only=True)
    distrito = serializers.CharField(source='id_ubicacion.distrito', read_only=True)
    localidad = serializers.CharField(source='id_ubicacion.localidad', read_only=True)
    evento = serializers.CharField(source='id_evento.evento', read_only=True)

    class Meta:
        model = HechosAsistenciaHumanitaria
        fields = [
            'fecha', 'departamento', 'distrito', 'localidad', 'evento',
            'kit_sentencia', 'kit_evento', 'chapa_fibrocemento_cantidad',
            'chapa_zinc_cantidad', 'colchones_cantidad', 'frazadas_cantidad', 'terciadas_cantidad',
            'puntales_cantidad', 'carpas_plasticas_cantidad'
        ]

class TotalAyudasSerializer(serializers.Serializer):
    # Serializador para los resultados agregados de las vistas de resumen
    anio = serializers.IntegerField(required=False)
    mes = serializers.IntegerField(required=False)
    nombre_mes = serializers.CharField(required=False, max_length=20)
    departamento = serializers.CharField(required=False, max_length=50)
    orden = serializers.IntegerField(required=False)
    distrito = serializers.CharField(required=False, max_length=50)
    evento = serializers.CharField(required=False, max_length=50)
    tipoEvento = serializers.CharField(required=False, max_length=50)
    numeroOcurrencias = serializers.IntegerField(required=False)
    kit_sentencia = serializers.IntegerField(required=False)
    kit_evento = serializers.IntegerField(required=False)
    chapa_fibrocemento_cantidad = serializers.IntegerField(required=False)
    chapa_zinc_cantidad = serializers.IntegerField(required=False)
    colchones_cantidad = serializers.IntegerField(required=False)
    frazadas_cantidad = serializers.IntegerField(required=False)
    terciadas_cantidad = serializers.IntegerField(required=False)
    puntales_cantidad = serializers.IntegerField(required=False)
    carpas_plasticas_cantidad = serializers.IntegerField(required=False)
    localidad = serializers.CharField(required=False, max_length=100)
    numero_eventos = serializers.IntegerField(required=False)
    numero_asistencias = serializers.IntegerField(required=False)
    unidades_distribuidas = serializers.SerializerMethodField()

    def get_unidades_distribuidas(self, obj):
        # Si el serializer recibe un contexto con 'producto', solo devuelve ese campo
        producto = self.context.get('producto') if hasattr(self, 'context') else None
        if producto and producto in obj:
            return obj.get(producto, 0) or 0
        # Si no, suma todos los campos (comportamiento general)
        return sum([
            obj.get('kit_sentencia', 0) or 0,
            obj.get('kit_evento', 0) or 0,
            obj.get('chapa_fibrocemento_cantidad', 0) or 0,
            obj.get('chapa_zinc_cantidad', 0) or 0,
            obj.get('colchones_cantidad', 0) or 0,
            obj.get('frazadas_cantidad', 0) or 0,
            obj.get('terciadas_cantidad', 0) or 0,
            obj.get('puntales_cantidad', 0) or 0,
            obj.get('carpas_plasticas_cantidad', 0) or 0
        ])

class ResumenGeneralSerializer(serializers.Serializer):
    cantidad_registros_total = serializers.IntegerField()
    cantidad_kit_evento = serializers.IntegerField()
    cantidad_departamentos = serializers.IntegerField()

class ResumenPorDepartamentoSerializer(serializers.Serializer):
    departamento = serializers.CharField()
    kit_sentencia = serializers.IntegerField(required=False)
    kit_evento = serializers.IntegerField(required=False)
    chapa_fibrocemento_cantidad = serializers.IntegerField(required=False)
    chapa_zinc_cantidad = serializers.IntegerField(required=False)
    colchones_cantidad = serializers.IntegerField(required=False)
    frazadas_cantidad = serializers.IntegerField(required=False)
    terciadas_cantidad = serializers.IntegerField(required=False)
    puntales_cantidad = serializers.IntegerField(required=False)
    carpas_plasticas_cantidad = serializers.IntegerField(required=False)
    cantidad_registros = serializers.IntegerField()
    evento_mas_frecuente = serializers.CharField()

class DistribucionAnualProductoSerializer(serializers.Serializer):
    anio = serializers.IntegerField()
    unidades_distribuidas = serializers.IntegerField()