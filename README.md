# predict_binary_h2o

Ejemplo mínimo de como construir un fat jar que tenga embebida la librería de h2o para poder predecir en spark.

El jar generado vale para los modelos de clasificación binaria de h2o

## Uso 

### Ejemplo 

Ejemplo con 10 ejecutores (+ 1 para del driver), 5 cores por ejecutor, y 10GB por ejecutor. Los argumentos que se le deben pasar son el path del modelo mojo entrenado, el nombre de la tabla dónde se va a aplicar el modelo y el nombre de la tabla a crear. Este jar crea una nueva tabla dónde guarda la primera columna de la tabla original y la columna con la probabilidad estimada

```bash
spark-submit \
--class com.h2ospark.BinaryModels \
--master yarn --num-executors 11 \
--executor-cores 5 \
--driver-memory 2G \
--executor-memory 10G  \
binary_h2o-1.0.3-SNAPSHOT.jar  \
path_mi_modelo_mojo.zip \
esquema.tabla_to_predict esquema.tabla_a_crear
``` 

