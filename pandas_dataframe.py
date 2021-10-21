import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

df = pd.read_csv("/home/aman/Downloads/archive/heart.csv")

# df_sex = df.groupby(["sex", "target"]).size()
# print(df_sex)
#
# plt.pie(df_sex.values, labels = ["sex_0,target_0", "sex_0,target_1", "sex_1,target_0", "sex_1,target_1"],autopct='%1.1f%%',radius = 1.5, textprops = {"fontsize" : 16})
# plt.show()


df.thal = df.thal.map({1:"normal", 2:"fixed defect", 3: "reversable defect"})
# df = pd.get_dummies(df, drop_first=True)
# print(df)
# print(type(df))
# print(df.columns)
df.sex = df.sex.map({1:"male", 0:"female"})
dummy=pd.get_dummies(df['sex'])
print(dummy)

df=pd.concat([df,dummy], axis=1)
print(df)