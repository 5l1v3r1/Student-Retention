import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go

path = 'GRADEBOOK_DATA_ALL_SCHOOLS_ALL_CLASSES_LAST_FIVE_YEARS.CSV'
data = pd.read_csv(path, encoding = "ISO-8859-1")

classes_we_want = ['9-CAMBRIDGE', '10-CAMBRIDGE', '11-CAMBRIDGE', '9-Matric', '10-Matric']
data = data[data.CLASS_NAME.isin(classes_we_want)]
data.drop(data.columns[[0, 8, 10, 12, 14, 16, 18, 22]], inplace=True, axis=1)
data['REGION_NAME'] = data.REGION_NAME.str.replace('CSO - ', '')
categorical_columns = ['REGION_NAME', 'CLUSTER_NAME', 'BR_NAME', 'YEAR_TITLE', 'TERM_NAME', 'CLASS_NAME', 'SECTION_NAME', 'SUBJECT_NAME', 'EXAM_TYPE']
for column in categorical_columns:
    data[column] = data[column].astype('category')
data['Stream'] = data['CLASS_NAME'].map({'9-CAMBRIDGE': 'O Level', '10-CAMBRIDGE': 'O Level', '11-CAMBRIDGE': 'O Level', '9-Matric': 'Matric', '10-Matric': 'Matric'})

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

#9 Number of students in 10-C

df = data.copy()
df9 = df[df.CLASS_NAME == '10-CAMBRIDGE']
df9 = df9.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df9.rename(columns={'SYSTEM_ID': 'Number of Students in 10-C'}, inplace=True)

#10 Number of students in 10-M

df = data.copy()
df10 = df[df.CLASS_NAME == '10-Matric']
df10 = df10.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df10.rename(columns={'SYSTEM_ID': 'Number of Students in 10-M'}, inplace=True)

#11 Number of students in 11-C

df = data.copy()
df11 = df[df.CLASS_NAME == '11-CAMBRIDGE']
df11 = df11.groupby(grouper).SYSTEM_ID.nunique().reset_index()
df11.rename(columns={'SYSTEM_ID': 'Number of Students in 11-C'}, inplace=True)

# Mering the dfs
dfs = [df2, df3, df4, df5, df6, df7, df8, df9, df10, df11]
for frame in dfs:
  print(f'Individuals: {frame.shape} and it is {frame.columns[-1]}')
df_final = df1.copy()
for frame in dfs:
  df_final = df_final.merge(frame, on=grouper, how='outer')
  print(f'Merged: {df_final.shape}')

print(f'final: {df_final.shape}')

# df_final.to_excel('Final_data_9-11_retention.xlsx', index=False)

final = df_final.copy()

grouped = final.groupby('YEAR_TITLE').sum().reset_index().drop('BRANCH_ID', axis=1)

grouped.YEAR_TITLE.unique()

#adding the year column for easy execution of the functions that follow
grouped['year'] = grouped['YEAR_TITLE'].map({'Aug, 17 - Jul, 18': 2017, 
                                         'Aug, 18 - Jul, 19': 2018,
                                         'Aug,14 - Jul,15': 2014,
                                         'Aug,15 - Jul,16': 2015,
                                         'Aug,16 - Jul,17': 2016})

def getwaterfall(batch=2017, base=3000):
  frame = grouped[grouped.year == batch].copy()
  first = str(batch)
  first = first[2:]
  second = str(int(first) + 1)
  title_ = 'O Level Batch of ' + str(batch) + '-' + second
  nine = int(frame['Number of Students in 9-C'])

  ten = grouped[grouped.year == batch+1]
  dropouts_in_10c = nine - int(ten['Students retained in 10-C'])
  new_students_in_10c = int(ten['New Students in 10-C'])
  total_in_10c = int(ten['Number of Students in 10-C'])
  
  eleven = grouped[grouped.year == batch+2]
  dropouts_in_11c = total_in_10c - int(eleven['Students retained in 11-C'])
  new_students_in_11c = int(eleven['New students in 11-C'])
  total_in_11c = int(eleven['Number of Students in 11-C'])

  numbers = [nine-base, dropouts_in_10c*-1, new_students_in_10c, None, dropouts_in_11c*-1, new_students_in_11c, None]
  texts = [nine, dropouts_in_10c, new_students_in_10c, total_in_10c, dropouts_in_11c, new_students_in_11c, total_in_11c]
  texts = [str(i) for i in texts]
  fig = go.Figure(go.Waterfall(
    x = [["9-C", "10-C", "10-C", "10-C", "11-C", "11-C", "11-C"],
       ["Enrolled in 9-C", "Dropouts", "New", "total", "Dropouts", "New", "total"]],
    measure = ["absolute", "relative", "relative", "total", "relative", "relative", "total"],
    y = numbers, base = base,
    text=texts,
    textposition="auto",
    decreasing = {"marker":{"color":"#DC143C"}},
    increasing = {"marker":{"color":"#50C878"}},
    totals = {"marker":{"color":"deep sky blue"}}
  ))

  fig.update_layout(title = title_, waterfallgap = 0.5, width=1000, paper_bgcolor="white")
  fig.update_yaxes(title_text='Number of Students')
  fig.show()

getwaterfall(2016)
