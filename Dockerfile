# Dockerfile para proyecto Django
FROM python:3.11-slim

# Variables de entorno para evitar prompts de Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Crear directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de requerimientos
COPY requirements.txt /app/

# Instalar dependencias de Python
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el resto del código
COPY . /app/

# Exponer el puerto (ajusta si usas otro)
EXPOSE 8000

# Comando para producción (Gunicorn)
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
