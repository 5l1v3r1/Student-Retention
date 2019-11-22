import os
import numpy as np
import pandas as pd

path = 'path_to_file'
data = pd.read_csv(path, encoding="ISO-8859-1")

classes_we_want = [
    '9-CAMBRIDGE', '10-CAMBRIDGE', '11-CAMBRIDGE', '9-Matric', '10-Matric'
]
data = data[data.CLASS_NAME.isin(classes_we_want)]
data.drop(data.columns[[0, 8, 10, 12, 14, 16, 18, 22]], inplace=True, axis=1)
data['REGION_NAME'] = data.REGION_NAME.str.replace('CSO - ', '')
categorical_columns = [
    'REGION_NAME', 'CLUSTER_NAME', 'BR_NAME', 'YEAR_TITLE', 'TERM_NAME',
    'CLASS_NAME', 'SECTION_NAME', 'SUBJECT_NAME', 'EXAM_TYPE'
]
for column in categorical_columns:
    data[column] = data[column].astype('category')
data['Stream'] = data['CLASS_NAME'].map({
    '9-CAMBRIDGE': 'O Level',
    '10-CAMBRIDGE': 'O Level',
    '11-CAMBRIDGE': 'O Level',
    '9-Matric': 'Matric',
    '10-Matric': 'Matric'
})

grouper = ["YEAR_TITLE", "REGION_NAME", "CLUSTER_NAME", "BRANCH_ID", "BR_NAME"]

# Now we work toward creating our df:
# We will have multiple dfs with a similar multi-index and then we will concat them together later on
# 1. Number of unique students in 9-C
df = data.copy()
df1 = df[df.CLASS_NAME == '9-CAMBRIDGE']
df1 = df1.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df1.rename(columns={'SYSTEM_ID': 'Number of Students in 9-C'}, inplace=True)

# 2. Number of students in 9-Matric
df = data.copy()
df2 = df[df.CLASS_NAME == '9-Matric']
df2 = df2.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df2.rename(columns={'SYSTEM_ID': 'Number of students in 9-M'}, inplace=True)

#3. Students retained in 10-C
df = data.copy()
std_9c = df[df.CLASS_NAME == '9-CAMBRIDGE'].SYSTEM_ID.unique()
df3 = df[df.CLASS_NAME == '10-CAMBRIDGE']
df3 = df3[df3.SYSTEM_ID.isin(std_9c)]
df3 = df3.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df3.rename(columns={'SYSTEM_ID': 'Students retained in 10-C'}, inplace=True)

# 4. Students retained in 10-M
df = data.copy()
std_9m = df[df.CLASS_NAME == '9-Matric'].SYSTEM_ID.unique()
df4 = df[df.CLASS_NAME == '10-Matric']
df4 = df4[df4.SYSTEM_ID.isin(std_9m)]
df4 = df4.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df4.rename(columns={'SYSTEM_ID': 'students retained in 10-M'}, inplace=True)

# 5. new students in 10-C
df = data.copy()
df5 = df[df.CLASS_NAME == '10-CAMBRIDGE']
df5 = df5[~df5.SYSTEM_ID.isin(std_9c)]
df5 = df5.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df5.rename(columns={'SYSTEM_ID': 'New Students in 10-C'}, inplace=True)

# 6. New students in 10-M
df = data.copy()
df6 = df[df.CLASS_NAME == '10-Matric']
df6 = df6[~df6.SYSTEM_ID.isin(std_9m)]
df6 = df6.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df6.rename(columns={'SYSTEM_ID': 'New students in 10-M'}, inplace=True)

# 7. Students retained in 11-C
df = data.copy()
std_10c = df[df.CLASS_NAME == '10-CAMBRIDGE'].SYSTEM_ID.unique()
df7 = df[df.CLASS_NAME == '11-CAMBRIDGE']
df7 = df7[df7.SYSTEM_ID.isin(std_10c)]
df7 = df7.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df7.rename(columns={'SYSTEM_ID': 'Students retained in 11-C'}, inplace=True)

# 8. New students in 11-C
df = data.copy()
df8 = df[df.CLASS_NAME == '11-CAMBRIDGE']
df8 = df8[~df8.SYSTEM_ID.isin(std_10c)]
df8 = df8.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df8.rename(columns={'SYSTEM_ID': 'New students in 11-C'}, inplace=True)

# Mering the 8 dfs
dfs = [df2, df3, df4, df5, df6, df7, df8]
df_final = df1.copy()
for frame in dfs:
    df_final = df_final.merge(frame, on=grouper, how='outer')
