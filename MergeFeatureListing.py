from json_flatten import flatten
import pandas as pd
import os, sys


def flt(dic):
    # Flatten with features columns
    features2 = {}
    basic_features = dic['basic']
    detailed_features = dic['det']
    top_features = dic['top']

    features2['basic'] = basic_features
    features2['det'] = {k: '1' for k in detailed_features}
    features2['top'] = {k: {v: '1' for v in top_features[k]} for k in top_features}
    flat2 = pd.DataFrame(flatten(features2), index=[0])
    return features2


os.chdir(os.path.expanduser('~/OneDrive - Kennesaw State University/My PyCharm Projects/AutoDataLoop Data/Features/Sec4'))
os.getcwd()

F = pd.read_csv('features_all_merged.csv')
L = pd.read_csv('Sec4_listings_cleaned.csv')

# f1 = F['features'].map(eval).map(flatten)
# y1 = f1.apply(pd.Series)
# print(y1)
#
# f2 = F['features'].map(eval).map(flt)
# y2 = f2.apply(pd.Series)
# print(y2)

M = F.merge(L.drop_duplicates(['vin'], keep='first'), on='vin', how='left')
M.to_csv('listings_features.csv',index=False)


