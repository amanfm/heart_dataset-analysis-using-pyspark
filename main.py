import numpy as np
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName('Heart_Data_Analysis').getOrCreate()

heart_dataset = spark.read.csv('/home/aman/Downloads/archive/heart.csv', header="true")
heart_dataset.show(4)

gender=heart_dataset.filter(heart_dataset.target == 1) \
    .groupBy(heart_dataset.sex, heart_dataset.target) \
    .count()
    # .show()

pd_gender = gender.toPandas()


avg_chol_with_heart_disease = heart_dataset.filter(heart_dataset.target == 1) \
    .groupBy(heart_dataset.target) \
    .agg({"chol": "avg"}) \
    .show()

gender_both = heart_dataset.groupBy(heart_dataset.sex) \
    .count() \
    .show()

pd_df = heart_dataset.toPandas()

sns.countplot(x="target", data=pd_df,hue = 'sex')
# sns.countplot(pd_gender.target, hue=pd_gender.sex)
plt.xlabel('Target')
plt.ylabel('Count')
plt.title('Target & Sex Counter')
plt.show()

df_sex = pd_df.groupby(["sex", "target"]).size()
print(df_sex)

# plt.pie(df_sex.values, labels = ["sex_0,target_0", "sex_0,target_1", "sex_1,target_0", "sex_1,target_1"],autopct='%1.1f%%',radius = 1.5, textprops = {"fontsize" : 16})
# plt.show()

pd_df_new = heart_dataset.toPandas()
pd_df_new.sex = pd_df_new.sex.map({0: "Female", 1: "Male"})

# print(pd_df_new)
# pd_df_new.printSchema()

avg_thalach = heart_dataset.agg({"thalach": "avg"}) \
    .show()


print('The Oldest Person age is', pd_df.age.max())
print('The Youngest Person age is', pd_df.age.min())

# plt.figure(figsize = (15,8))
# sns.swarmplot(x = 'age',data = pd_df)
# plt.xlabel('Age', fontsize = 15)
# plt.ylabel('Number of People', fontsize = 15)
# plt.title('Frequency', fontsize = 20)
# plt.show()

# cp_data = (pd_df.groupby(['target']))['cp'].value_counts(normalize=True)\
#    .mul(100).reset_index(name = "percentage")
#
# sns.barplot(x = "target", y = "percentage", hue = "cp", data = cp_data)
# plt.title("Chest Pain types  with Heart Disease")
# plt.show()