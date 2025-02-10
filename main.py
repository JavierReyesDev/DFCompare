import os
import pandas as pd
from data import CUTOFF_FIELDS, SEPARATORS


def get_paths():
    paths = []
    for table in dirs:
        instances = os.listdir(f"csv/{table}")
        for instance in instances:
            paths.append(f"csv/{table}/{instance}")
    return paths


def compare_values(df1, df2, folder, path_df1, path_df2):
    df1[CUTOFF_FIELDS[folder][0]] = df1[CUTOFF_FIELDS[folder][0]].str.strip().str.lower()
    df2[CUTOFF_FIELDS[folder][1]] = df2[CUTOFF_FIELDS[folder][1]].str.strip().str.lower()

    # Columns are renamed before merging to avoid automatic alignment (in case that cutoff_fields are identic)
    df1_renamed = df1.rename(columns={CUTOFF_FIELDS[folder][0]: path_df1})
    df2_renamed = df2.rename(columns={CUTOFF_FIELDS[folder][1]: path_df2})

    # Outer join
    compared_df = pd.merge(
        left=df1_renamed,
        right=df2_renamed,
        how="outer",
        left_on=path_df1,
        right_on=path_df2,
        indicator=True
    )

    # Mapping, renaming and writing
    compared_df["_merge"] = compared_df["_merge"].replace(
        {
            "left_only": f"In {path_df1} only",
            "right_only": f"In {path_df2} only",
            "both": "In both DataFrames"
        }
    ).astype("category")
    compared_df = compared_df[[path_df1, path_df2, "_merge"]]
    compared_df = compared_df.rename(columns={"_merge": "LOCATION"})

    write_files(compared_df, folder)


def compare_dfs(folder, path_lists):
    path_df1 = path_lists.pop(0)
    path_df2 = path_lists.pop(0)

    df1 = pd.read_csv(path_df1, sep=SEPARATORS[folder][0]).sort_values(by=CUTOFF_FIELDS[folder][0])
    df2 = pd.read_csv(path_df2, sep=SEPARATORS[folder][1]).sort_values(by=CUTOFF_FIELDS[folder][1])

    name_df1 = path_df1.split("/")[-1].split(".")[0]
    name_df2 = path_df2.split("/")[-1].split(".")[0]
    compare_values(df1, df2, folder, name_df1, name_df2)
    print(f"[Table {folder} was processed correctly]")


def write_files(compared_df, nombre_tabla):
    compared_df.to_excel(f"diff/xlsx/{nombre_tabla}_diff.xlsx", index=False)
    compared_df.to_csv(f"diff/csv/{nombre_tabla}_diff.csv", index=False)


dirs = os.listdir("csv")
all_paths = get_paths()

for index in range(0, len(dirs)):
    dir_ = dirs[index]
    try:
        compare_dfs(dir_, all_paths)
    except KeyError:
        print(f"Problem with {dir_}")
        print("The keys are incorrect")
