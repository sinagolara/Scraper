from json_flatten import flatten
import pandas as pd


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

data = pd.read_csv('./features_all_merged.csv')

f1 = data['features'].map(eval).map(flatten)
f2 = data['features'].map(eval).map(flt)
y1 = f1.apply(pd.Series)
y2 = f2.apply(pd.Series)
print(y1)
print(y2)
