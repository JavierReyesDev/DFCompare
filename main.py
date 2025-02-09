import os
import pandas as pd

# USER SETS CUTOFF_FIELDS AND SEPARATORS

CUTOFF_FIELDS_TABLES = {
    "fruits": ["fruit", "fruit"],
    "vegetables": ["vegetable", "veggie"]
}

SEPARATORS = {
    "fruits": [",", ","],
    "vegetables": [",", ","],
}


def get_paths():
    paths = []
    for table in dirs:
        instances = os.listdir(f"csv/{table}")
        for instance in instances:
            paths.append(f"csv/{table}/{instance}")
    return paths


dirs = os.listdir("csv")
all_paths = get_paths()


def write_files(df_filtered, nombre_tabla):
    # Guardar el archivo Excel con las diferencias
    df_filtered.to_excel(f"diff/xlsx/{nombre_tabla}_diff.xlsx", index=False)
    df_filtered.to_csv(f"diff/csv/{nombre_tabla}_diff.csv", index=False)


def compare_values(df1, df2, dir, path_df1, path_df2):
    # Removing extra spaces & turning cutoff_fields to lowercase
    df1[CUTOFF_FIELDS_TABLES[dir][0]] = df1[CUTOFF_FIELDS_TABLES[dir][0]].str.strip().str.lower()
    df2[CUTOFF_FIELDS_TABLES[dir][1]] = df2[CUTOFF_FIELDS_TABLES[dir][1]].str.strip().str.lower()

    compared_df = pd.merge(left=df1,
                           right=df2,
                           how='outer',
                           left_on=CUTOFF_FIELDS_TABLES[dir][0],
                           right_on=CUTOFF_FIELDS_TABLES[dir][1],
                           indicator=True
                           )

    # compared_df['_merge'] = compared_df['_merge'].replace({
    #     'left_only': f'In {path_df1} only',
    #     'right_only': f'In {path_df2} only',
    #     'both': 'In both DataFrames'
    # })

    # Ensure that '_merge' column is categorical
    compared_df['_merge'] = compared_df['_merge'].astype('category')

    # Rename the categories directly
    compared_df['_merge'] = compared_df['_merge'].cat.rename_categories({
        'left_only': f'In {path_df1} only',
        'right_only': f'In {path_df2} only',
        'both': 'In both DataFrames'
    })

    # Reordenar las columnas para que '_merge' sea la primera
    compared_df = compared_df[[CUTOFF_FIELDS_TABLES[dir][0], '_merge']]
    compared_df = compared_df.rename(
        columns={
            CUTOFF_FIELDS_TABLES[dir][0]: "FIELD",
            "_merge": "LOCATION"
        }
    )
    df_filtered = compared_df.dropna()

    write_files(df_filtered, dir)


def compare_dfs(dir, path_lists):
    if len(path_lists) < 2:
        print(f"Error: No hay suficientes rutas para comparar para {dir}.")
        return
    else:
        # Los paths de df1 y df2 deben ser tomados en pares consecutivos
        path_df1 = path_lists.pop(0)
        path_df2 = path_lists.pop(0)

        # Leer los CSV asegurando que cada uno tiene el delimitador correcto
        df1 = pd.read_csv(path_df1, sep=SEPARATORS[dir][0]).sort_values(by=CUTOFF_FIELDS_TABLES[dir][0])
        df2 = pd.read_csv(path_df2, sep=SEPARATORS[dir][1]).sort_values(by=CUTOFF_FIELDS_TABLES[dir][1])

        name_df1 = path_df1.split("/")[-1].split(".")[0]
        name_df2 = path_df2.split("/")[-1].split(".")[0]

        compare_values(df1, df2, dir, name_df1, name_df2)
        print(f"[Table {dir} was processed correctly]")


# Comparar las tablas en pares de 2
for index in range(0, len(dirs)):
    dir_ = dirs[index]
    try:
        compare_dfs(dir_, all_paths)
    except KeyError:
        print(f"Problem with {dir_}")
        print("The keys are incorrect")
