import pandas as pd
from data import tables_, instances

# campos
FIELD_DF1 = ""
FIELD_DF2 = ""

# paths
def get_paths():
    paths = []
    for t in tables_:
        for instance in instances:
            path = f"csv/{t}/{instance}"
            paths.append(path)
    return paths
    # TODO: include xslx

# diff
# def comparar_numero_registros(len_df1, len_df2):
#     if len_df1 == len_df2:
#         print(f"1. Mismo número de registros\n > df1: {len_df1} records\n > df2: {len_df2} registros")
#     else:
#         print(f"1. Distinto número de registros\n > df1: {len_df1} records\n > df2: {len_df2} registros")


def comparar_mismos_valores(df1, df2, nombre_tabla):
    # Limpiar los valores de las columnas para evitar diferencias por espacios o mayúsculas/minúsculas
    df1[FIELD_DF1] = df1[FIELD_DF1].str.strip().str.lower()
    df2[FIELD_DF2] = df2[FIELD_DF2].str.strip().str.lower()

    # Realizar el merge para comparar con las columnas correspondientes de cada DataFrame
    df_comparado = pd.merge(df1, df2, how='outer',
                            left_on=FIELD_DF1, right_on=FIELD_DF2,
                            indicator=True)

    # Reemplazar el indicador con descripciones claras
    df_comparado['_merge'] = df_comparado['_merge'].replace({
        'left_only': 'Solo en df1',
        'right_only': 'Solo en df2',
        'both': 'En ambos DataFrames'
    })

    # Reordenar las columnas para que '_merge' sea la primera
    cols = ['_merge'] + [col for col in df_comparado.columns if col != '_merge']
    df_comparado = df_comparado[cols]

    # Guardar el archivo Excel con las diferencias
    df_comparado.to_excel(f"diff/{nombre_tabla}_diff.xlsx", index=False, engine='openpyxl')


def compare_dfs(nombre_tabla, path_lists):
    if len(path_lists) < 2:
        print(f"Error: No hay suficientes rutas para comparar para {nombre_tabla}.")
        return

    # Los paths de df1 y df2 deben ser tomados en pares consecutivos
    path_df1 = path_lists.pop(0)
    path_df2 = path_lists.pop(0)

    # Leer los CSV asegurando que cada uno tiene el delimitador correcto
    df1 = pd.read_csv(path_df1, sep=',').sort_values(by=FIELD_DF1)
    df2 = pd.read_csv(path_df2, sep=';').sort_values(by=FIELD_DF2)

    print("___________________________________")
    print(f"TABLA: {nombre_tabla}")
    print("---")
    print("---")
    # COMPARACIÓN DE LOS VALORES (COLUMNAS 'NAMING' y 'PHYSICAL_NAME')
    comparar_mismos_valores(df1, df2, nombre_tabla)
    print("---")


# Obtener todos los paths
all_paths = get_paths()

# Comparar las tablas en pares de 2
for index in range(0, len(tables_)):
    tabla = tables_[index]
    compare_dfs(tabla, all_paths)
