import sqlite3
import pandas as pd 
from itertools import combinations

# conectar a las base de datos SQLite
conexion = sqlite3.connect('D:/Analyst/DS4B/sanoyfresco/sanoyfresco.db')

print('Conexión exitosa a la base de datos')

# Cargar una tabla completa en un DataFrame de pandas 
df = pd.read_sql_query("SELECT * FROM tickets", conexion)

print('Tabla cargada en DataFrame')

# Mostrar los primeros 5 registros del DataFrame
print(df.head(5))
print('-------------------------------')
#print(df.last(5))

# Cerrar la conexión a la base de datos
conexion.close()

print('Conexión a la base de datos cerrada')

# Mostrar información del DataFrame
print(df.info())

# Cambiar el tipo de datos de la columna 'fecha' a datetime
df['fecha'] = pd.to_datetime(df['fecha'])

print('Tipo de datos de la columna "fecha" cambiada a datetime')

print(df.info())

# Montar una tabla con id_pedido y nombre_producto
df_cesta = df[['id_pedido', 'nombre_producto']]

print(df_cesta.head(5))
print('--------------------------------')
print(df_cesta.tail(5))

# Agrupar los pedidos por id_pedido
df_agrupado = df_cesta.groupby('id_pedido')['nombre_producto'].apply(lambda producto: ','.join(producto))

print(df_cesta.head(5))
print('--------------------------------')
print(df_cesta.tail(5))

# Aplicar pd.get_dumnies para transformar los productos en columnas con 0/1
df_transacciones = df_agrupado.str.get_dummies(sep=',') 

print(df_transacciones.head(5))
print('--------------------------------')
print(df_transacciones.tail(5))

# Soporte para cada producto
soporte = df_transacciones.mean() *100
soporte_sort = soporte.sort_values(ascending=False)
print(soporte_sort)

# Funciones para calcular la confianza entre dos productos en la muestra
def confianza(antecedente, consecuente):
    # caso donde se compraram ambos productos
    conjunto_ac = df_transacciones[(df_transacciones[antecedente] == 1) &
                                   (df_transacciones[consecuente] == 1)]
    # confianza = compras conjuntas / compras del producto A
    return len(conjunto_ac) / df_transacciones[antecedente].sum()

# Funciones para calcular el lift entre dos productos en la muestra
def lift(antecedente, consecuente):
    soporte_a = df_transacciones[antecedente].mean()
    soporte_c = df_transacciones[consecuente].mean()
    conteo_ac = len(df_transacciones[(df_transacciones[antecedente] ==1) &
                                     (df_transacciones[consecuente] == 1)])
    soporte_ac = conteo_ac / len(df_transacciones)
    return soporte_ac / (soporte_a * soporte_c)

# Definir un umbral para confianza minima
umbral_confianza = 0.05
asociaciones =[]

# Generar combinaciones de productos y calcular confianza y Lift
for antecedente, consecuente in combinations(df_transacciones.columns, 2):

    # soporte del antecedente
    soporte_a = df_transacciones[antecedente].mean()

    # calcular confianza
    conf = confianza(antecedente, consecuente)
    if conf > umbral_confianza:
        asociaciones.append({
            'antecedente': antecedente,
            'consecuente': consecuente,
            'soporte_a': round(soporte_a*100, 1),
            'confianza': round(conf*100, 1),
            'lift': round(lift(antecedente, consecuente), 1)
        })
    
print('Productos asociados')

# Convertir las asociaciones en un DataFrame
df_asociaciones = pd.DataFrame(asociaciones)

print('Convertido a DataFrame')

# Ordenar las asociaciones por confianza de mayor a menor
df_asociaciones = df_asociaciones.sort_values(by='lift', ascending=False)#, inplace=True)
    
print(df_asociaciones)

# Crear una tabla con los productos únicos e las columnas correspondientes
productos_unicos = df[['id_producto', 'id_seccion', 'id_departamento', 'nombre_producto']].drop_duplicates()

print(productos_unicos.head(20))

# Crear una tabla com asociaciones e produtos únicos
df_asociaciones_enriquecida = df_asociaciones.merge(
    productos_unicos, 
    left_on='antecedente', 
    right_on='nombre_producto', 
    how='left').drop(columns=['nombre_producto'])

df_asociaciones_enriquecida.columns = [
    'antecedente',
      'consecuente', 
      'soporte_a', 
      'confianza', 
      'lift', 
      'id_producto_a',
      'id_seccion_a',
      'id_departamento_a'
]

print(df_asociaciones_enriquecida.head(20))

# Transformar df_asociaciones_enriquecida em .csv
df_asociaciones_enriquecida.to_csv('D:/Analyst/DS4B/sanoyfresco/reglas.csv', index=False, sep=';', decimal=',')
print('Archivo CSV de asociaciones creado con éxito')