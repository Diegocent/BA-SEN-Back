from django.db import models

class DimFecha(models.Model):
    id_fecha = models.AutoField(primary_key=True, db_column='id_fecha')
    fecha = models.DateField(unique=True, db_column='fecha')
    anio = models.IntegerField(db_column='anio')
    mes = models.IntegerField(db_column='mes')
    nombre_mes = models.TextField(db_column='nombre_mes')
    dia_del_mes = models.IntegerField(db_column='dia_del_mes')

    class Meta:
        db_table = 'dim_fecha'
        managed = False

class DimUbicacion(models.Model):
    id_ubicacion = models.AutoField(primary_key=True, db_column='id_ubicacion')
    departamento = models.TextField(db_column='departamento')
    distrito = models.TextField(db_column='distrito')
    localidad = models.TextField(db_column='localidad')
    orden = models.IntegerField(db_column='orden')

    class Meta:
        db_table = 'dim_ubicacion'
        managed = False
        unique_together = (('departamento', 'distrito', 'localidad'),)

class DimEvento(models.Model):
    id_evento = models.AutoField(primary_key=True, db_column='id_evento')
    evento = models.TextField(unique=True, db_column='evento')

    class Meta:
        db_table = 'dim_evento'
        managed = False

class HechosAsistenciaHumanitaria(models.Model):
    id_asistencia_hum = models.AutoField(primary_key=True, db_column='id_asistencia_hum')
    id_fecha = models.ForeignKey(DimFecha, models.DO_NOTHING, db_column='id_fecha')
    id_ubicacion = models.ForeignKey(DimUbicacion, models.DO_NOTHING, db_column='id_ubicacion')
    id_evento = models.ForeignKey(DimEvento, models.DO_NOTHING, db_column='id_evento')
    kit_sentencia = models.IntegerField(null=True, blank=True)
    kit_evento = models.IntegerField(null=True, blank=True)
    chapa_fibrocemento_cantidad = models.IntegerField(null=True, blank=True)
    chapa_zinc_cantidad = models.IntegerField(null=True, blank=True)
    colchones_cantidad = models.IntegerField(null=True, blank=True)
    frazadas_cantidad = models.IntegerField(null=True, blank=True)
    terciadas_cantidad = models.IntegerField(null=True, blank=True)
    puntales_cantidad = models.IntegerField(null=True, blank=True)
    carpas_plasticas_cantidad = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'hechos_asistencia_humanitaria'
        managed = False
