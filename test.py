import pandas as pd
hist_data = pd.read_pickle("Input/historical_data/his_abnormal.pickle")
print(hist_data)
hist_data.to_csv("Input/historical_data/his_abnormal.csv", index=False, encoding='utf_8_sig')

hist_data2 = pd.read_csv("Input/historical_data/his_abnormal.csv", encoding='utf_8_sig')
print(hist_data2)