#Import libraries
import requests
import json
import numpy as np
import pandas as pd
import boto3
from io import StringIO

#key for IAM
IAM = pd.read_csv('HelloFresh_accessKeys.csv')
ACCESS_ID = IAM.iloc[0]['Access key ID']
ACCESS_KEY = IAM.iloc[0]['Secret access key']

def jsonToData(url):
    #Calling API
    response = requests.get(url)
    classicBox = json.loads(response.text)
    #JSON to pandas dataframe
    return pd.json_normalize(classicBox['items'],record_path=['courses'])


def cleanData(df):
    #Cleaning data
    df = df[df.columns.drop(list(df.filter(regex='chargeSetting.')))]
    df.drop('index', inplace=True, axis=1)
    df.drop('presets', inplace=True, axis=1)
    df.drop('selectionLimit', inplace=True, axis=1)
    df.drop('chargeSetting', inplace=True, axis=1)
    df.drop('isSoldOut', inplace=True, axis=1)
    df.drop('soldOutThreshold', inplace=True, axis=1)
    df.columns = df.columns.str.split('recipe.').str[1]
    df.drop('country', inplace=True, axis=1)
    df.drop('slug', inplace=True, axis=1)
    df.drop('imageLink', inplace=True, axis=1)
    df.drop('imagePath', inplace=True, axis=1)
    df.drop('isPremium', inplace=True, axis=1)
    df.drop('tags', inplace=True, axis=1)
    df.drop('category', inplace=True, axis=1)
    df.drop('cuisines', inplace=True, axis=1)
    df.drop('active', inplace=True, axis=1)
    df.drop('isDinnerToLunch', inplace=True, axis=1)
    df.drop('author', inplace=True, axis=1)
    df.drop('websiteUrl', inplace=True, axis=1)
    df.drop('steps', inplace=True, axis=1)
    df.drop('ingredients', inplace=True, axis=1)
    df.drop('yields', inplace=True, axis=1)
    df = df[df.columns.drop(list(df.filter(regex='label')))]
    #Exploding nutrition column
    df1 = pd.concat([pd.DataFrame(i) for i in df['nutrition']], keys = df['id']).reset_index(level=1,drop=True)
    df1.drop('type', inplace=True, axis=1)

    energy = df1[df1['name'] == "Energy (kJ)"].copy()
    energy.drop('name', inplace=True, axis=1)
    energy.drop('unit', inplace=True, axis=1)
    energy.rename(columns={"amount": "Energy (kJ)"}, inplace=True)

    fat = df1[df1['name'] == "Fat"].copy()
    fat.drop('name', inplace=True, axis=1)
    fat.drop('unit',inplace=True,axis=1)
    fat.rename(columns={"amount": "Fat (g)"}, inplace=True)

    protein = df1[df1['name'] == "Protein"].copy()
    protein.drop('name', inplace=True, axis=1)
    protein.drop('unit',inplace=True,axis=1)
    protein.rename(columns={"amount": "Protein (g)"}, inplace=True)

    carbohydrate = df1[df1['name'] == "Carbohydrate"].copy()
    carbohydrate.drop('name', inplace=True, axis=1)
    carbohydrate.drop('unit',inplace=True,axis=1)
    carbohydrate.rename(columns={"amount": "Carbohydrate (g)"}, inplace=True)


    ww_menu = pd.merge(df,energy,on='id')
    ww_menu = pd.merge(ww_menu,fat,on='id')
    ww_menu = pd.merge(ww_menu,protein,on='id')
    ww_menu = pd.merge(ww_menu,carbohydrate,on='id')
    ww_menu.drop('id',inplace=True,axis =1)
    ww_menu.drop('nutrition',inplace=True,axis =1)
    return ww_menu

#Flattening clean data to csv
def dataToCsv(df,bucket):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)
    s3_resource.Object(bucket,'YYYY_WW_menu.csv').put(Body=csv_buffer.getvalue())
    return

#Flattening aggregrated data to csv
def topTen(df,bucket):
    df['total'] = df['favoritesCount'] + df['ratingsCount']
    df = df.nlargest(10,['total'])
    df.drop('total',inplace=True,axis=1)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)
    s3_resource.Object(bucket,'YYYY_WW_TOP_10.csv').put(Body=csv_buffer.getvalue())
    return

if __name__ == '__main__':
    #initialise s3 bucket
    bucket = "hellofresh-s3-bucket"
    url = "https://hellofresh-au.free.beeceptor.com/menus/2021-W10/classic-box"
    df = cleanData(jsonToData(url))
    dataToCsv(df,bucket)
    topTen(df,bucket)
