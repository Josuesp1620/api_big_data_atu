# Usa una imagen base de Python
FROM python:3.10.13

# Establece el directorio de trabajo
WORKDIR /opt/backend_atu_api

# Copia los archivos necesarios al contenedor
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación al contenedor
COPY . .

# Expone el puerto en el que se ejecutará la aplicación Flask
EXPOSE 3071

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]
