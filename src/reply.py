# by MinCiencia, based in https://realpython.com/twitter-bot-python-tweepy/#the-reply-to-mentions-bot

import tweepy
import logging
from config_bot import create_api
import time
import pandas as pd
import numpy as np
import requests
import io
import sys


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def check_mentions(api, keywords, since_id):
    logger.info("Retrieving mentions")
    new_since_id = since_id
    my_files = {
        'activos':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto19/CasosActivosPorComuna.csv',
        'activos_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto19/CasosActivosPorComuna_T.csv',
        'vacunacion':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_1eraDosis.csv',
        'vacunacion1_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_1eraDosis_T.csv',
        'vacunacion2_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_2daDosis_T.csv',
        'vacunacion3_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_UnicaDosis_T.csv',
        'poblacion_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/poblacion_comuna_edad_T.csv',
        'vacunacion1_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_1eraDosis_T.csv',
        'vacunacion2_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_2daDosis_T.csv',
        'vacunacion3_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_UnicaDosis_T.csv',
        'vacunacionR_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_Refuerzo_T.csv'
    }
    for tweet in tweepy.Cursor(api.mentions_timeline,since_id=since_id,tweet_mode='extended').items():
        new_since_id = max(tweet.id, new_since_id)
        # if tweet.in_reply_to_status_id is not None:
        #     continue
        texto = tweet.full_text.lower()
        texto = texto.replace('ñ', 'n')
        texto = texto.replace('á', 'a')
        texto = texto.replace('é', 'e')
        texto = texto.replace('í', 'i')
        texto = texto.replace('ó', 'o')
        texto = texto.replace('ú', 'u')
        texto = texto.replace('ü', 'u')
        texto = texto.replace('?', '')
        texto = texto.replace('¿', '')
        if any(keyword in texto for keyword in keywords):
            comuna = texto.replace('@min_ciencia_ia ', '')
            if comuna in keywords:
                logger.info(f"Answering to {tweet.user.name}")
                #casos activos
                content = requests.get(my_files['activos']).content
                df = pd.read_csv(io.StringIO(content.decode('utf-8')))

                content = requests.get(my_files['activos_t']).content
                dfT = pd.read_csv(io.StringIO(content.decode('utf-8')))

                df["Comuna"] = df["Comuna"].str.lower()
                n = df.index[df['Comuna'] == comuna]
                casos_ultimo_informe = int(pd.to_numeric(dfT.iloc[dfT.index.max()][n + 1]))
                casos_informe_anterior = int(pd.to_numeric(dfT.iloc[dfT.index.max() - 1][n + 1]))
                variacion = casos_ultimo_informe - casos_informe_anterior
                fecha = dfT.iloc[dfT.index.max()][0]

                #vacunación
                content = requests.get(my_files['vacunacion']).content
                dfve1 = pd.read_csv(io.StringIO(content.decode('utf-8')))

                content = requests.get(my_files['vacunacion1_comuna_t']).content
                dfve1_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacion2_comuna_t']).content
                dfve2_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacion3_comuna_t']).content
                dfve3_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['poblacion_t']).content
                dfvep_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacion1_dosis_t']).content
                dfv1_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacion2_dosis_t']).content
                dfv2_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacion3_dosis_t']).content
                dfv3_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                content = requests.get(my_files['vacunacionR_dosis_t']).content
                dfvR_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                dfve1["Comuna"] = dfve1["Comuna"].str.lower()
                n = dfve1.index[dfve1['Comuna'] == comuna]

                #total poblacion objetivo de la comuna
                dfvep_T = dfvep_T[5:][n + 1]
                dfvep_T = dfvep_T.astype(float)
                tot = int(dfvep_T.sum())

                # Porcentaje 1era dosis
                dfve1_T = dfve1_T[5:][n + 1]
                dfve1_T = dfve1_T.astype(float)
                v1 = int(dfve1_T.sum())
                porcentaje1 = str(min(100, round(100 * v1 / tot)))

                # Porcentaje pauta completa
                dfve2_T = dfve2_T[5:][n + 1]
                dfve2_T = dfve2_T.astype(float)
                v2 = int(dfve2_T.sum())
                dfve3_T = dfve3_T[5:][n + 1]
                dfve3_T = dfve3_T.astype(float)
                v3 = int(dfve3_T.sum())
                porcentaje2 = str(min(100, round(100 * (v2 + v3) / tot)))

                # Porcentaje refuerzo
                dfveR_T = dfveR_T[5:][n + 1]
                dfveR_T = dfveR_T.astype(float)
                r1 = int(dfveR_T.sum())
                porcentaje3 = str(min(100, round(100 * r1 / tot)))

                # Rolling mean last week
                dfv1_T = dfv1_T[5:][n + 1]
                dfv2_T = dfv2_T[5:][n + 1]
                dfv3_T = dfv3_T[5:][n + 1]
                dfvR_T = dfvR_T[5:][n + 1]
                dfv1_T = dfv1_T.astype(float)
                dfv2_T = dfv2_T.astype(float)
                dfv3_T = dfv3_T.astype(float)
                dfvR_T = dfvR_T.astype(float)
                dft = dfv1_T + dfv2_T+dfv3_T + dfvR_T
                dft.reset_index(drop=True, inplace=True)
                dft = dft.rolling(7).mean().round(4)
                promedio = str(int(dft.iloc[dft.index.max() - 1][n + 1]))

                comuna = normalizaNombreComuna(comuna)
                if variacion > 0:
                    reply = "🤖Hola @" + tweet.user.screen_name + ". En " + comuna + " los casos activos de Covid19 son " + str(casos_ultimo_informe) + " con base en el último informe epidemiológico del @ministeriosalud (" + fecha + "), " + str(variacion) + " más que en el informe anterior."
                    reply2 = "🤖Además, acorde a la información de la campaña #YoMeVacuno ✌️, un " + porcentaje1 + "% de la población objetivo tiene su primera dosis, un " + porcentaje2 + "% tiene pauta completa, y un " + porcentaje3 + "% tiene dosis de refuerzo 🦾."
                    reply3 = "🤖Por último, un promedio diario de " + promedio + " personas han recibido su vacuna en " + comuna + " esta semana 🦾."
                    try:
                        update = api.update_status(status=reply, in_reply_to_status_id=tweet.id)
                        update2 = api.update_status(status=reply2, in_reply_to_status_id=update.id)
                        api.update_status(status=reply3, in_reply_to_status_id=update2.id)
                    except tweepy.TweepError as error:
                        if error.api_code == 187:
                            # Do something special
                            print('duplicate message')
                else:
                    reply = "🤖Hola @" + tweet.user.screen_name + ". En " + comuna + " los casos activos de Covid19 son " + str(casos_ultimo_informe) + " con base en el último informe epidemiológico del @ministeriosalud (" + fecha + "), " + str((-1) * variacion) + " menos que en el informe anterior."
                    reply2 = "🤖Además, acorde a la información de la campaña #YoMeVacuno ✌️, un " + porcentaje1 + "% de la población objetivo tiene su primera dosis, un " + porcentaje2 + "% tiene pauta completa, y un " + porcentaje3 + "% tiene dosis de refuerzo 🦾."
                    reply3 = "🤖Por último, un promedio diario de " + promedio + " personas han recibido su vacuna en " + comuna + " esta semana 🦾."
                    try:
                        update = api.update_status(status=reply, in_reply_to_status_id=tweet.id)
                        update2 = api.update_status(status=reply2, in_reply_to_status_id=update.id)
                        api.update_status(status=reply3, in_reply_to_status_id=update2.id)
                    except tweepy.TweepError as error:
                        if error.api_code == 187:
                            # Do something special
                            print('duplicate message')
            else:
                if tweet.in_reply_to_screen_name == 'min_ciencia_IA':
                    return new_since_id
                else:
                    for word in texto.lower().split():
                        word = word.replace('@min_ciencia_ia ', '')
                        word = word.replace(' @min_ciencia_ia', '')
                        if word in keywords:
                            logger.info(f"Answering to {tweet.user.name}")
                            # casos activos

                            content = requests.get(my_files['activos']).content
                            df = pd.read_csv(io.StringIO(content.decode('utf-8')))

                            content = requests.get(my_files['activos_t']).content
                            dfT = pd.read_csv(io.StringIO(content.decode('utf-8')))

                            df["Comuna"] = df["Comuna"].str.lower()
                            n = df.index[df['Comuna'] == word]
                            casos_ultimo_informe = int(pd.to_numeric(dfT.iloc[dfT.index.max()][n + 1]))
                            casos_informe_anterior = int(pd.to_numeric(dfT.iloc[dfT.index.max() - 1][n + 1]))
                            variacion = casos_ultimo_informe - casos_informe_anterior
                            fecha = dfT.iloc[dfT.index.max()][0]

                            # vacunación
                            content = requests.get(my_files['vacunacion']).content
                            dfve1 = pd.read_csv(io.StringIO(content.decode('utf-8')))

                            content = requests.get(my_files['vacunacion1_comuna_t']).content
                            dfve1_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['vacunacion2_comuna_t']).content
                            dfve2_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['vacunacion3_comuna_t']).content
                            dfve3_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['poblacion_t']).content
                            dfvep_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['vacunacion1_dosis_t']).content
                            dfv1_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['vacunacion2_dosis_t']).content
                            dfv2_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            content = requests.get(my_files['vacunacion3_dosis_t']).content
                            dfv3_T = pd.read_csv(io.StringIO(content.decode('utf-8')), header=None)

                            dfve1["Comuna"] = dfve1["Comuna"].str.lower()
                            n = dfve1.index[dfve1['Comuna'] == word]

                            # total poblacion objetivo de la comuna
                            dfvep_T = dfvep_T[5:][n + 1]
                            dfvep_T = dfvep_T.astype(float)
                            tot = int(dfvep_T.sum())

                            # Porcentaje 1era dosis
                            dfve1_T = dfve1_T[5:][n + 1]
                            dfve1_T = dfve1_T.astype(float)
                            v1 = int(dfve1_T.sum())
                            porcentaje1 = str(min(100,round(100 * v1 / tot)))

                            # Porcentaje pauta completa
                            dfve2_T = dfve2_T[5:][n + 1]
                            dfve2_T = dfve2_T.astype(float)
                            v2 = int(dfve2_T.sum())
                            dfve3_T = dfve3_T[5:][n + 1]
                            dfve3_T = dfve3_T.astype(float)
                            v3 = int(dfve3_T.sum())
                            porcentaje2 = str(min(100,round(100 * (v2+v3) / tot)))

                            # Porcentaje refuerzo
                            dfveR_T = dfveR_T[5:][n + 1]
                            dfveR_T = dfveR_T.astype(float)
                            r1 = int(dfveR_T.sum())
                            porcentaje3 = str(min(100, round(100 * r1 / tot)))

                            # Rolling mean last week
                            dfv1_T = dfv1_T[5:][n + 1]
                            dfv2_T = dfv2_T[5:][n + 1]
                            dfv3_T = dfv3_T[5:][n + 1]
                            dfv1_T = dfv1_T.astype(float)
                            dfv2_T = dfv2_T.astype(float)
                            dfv3_T = dfv3_T.astype(float)
                            dft = dfv1_T + dfv2_T + dfv3_T
                            dft.reset_index(drop=True, inplace=True)
                            dft = dft.rolling(7).mean().round(4)
                            promedio = str(int(dft.iloc[dft.index.max() - 1][n + 1]))

                            word = normalizaNombreComuna(word)

                            if variacion > 0:
                                reply = "🤖Hola @" + tweet.user.screen_name + ". En " + word + " los casos activos de Covid19 son " + str(
                                    casos_ultimo_informe) + " con base en el último informe epidemiológico del @ministeriosalud (" + fecha + "), " + str(
                                    variacion) + " más que en el informe anterior."
                                reply2 = "🤖Además, acorde a la información de la campaña #YoMeVacuno ✌️, un " + porcentaje1 + "% de la población objetivo tiene su primera dosis, un " + porcentaje2 + "% tiene pauta completa, y un " + porcentaje3 + "% tiene dosis de refuerzo 🦾."
                                reply3 = "🤖Por último, un promedio diario de " + promedio + " personas han recibido su vacuna en " + word + " esta semana 🦾."
                                try:
                                    update = api.update_status(status=reply, in_reply_to_status_id=tweet.id)
                                    update2 = api.update_status(status=reply2, in_reply_to_status_id=update.id)
                                    api.update_status(status=reply3, in_reply_to_status_id=update2.id)
                                except tweepy.TweepError as error:
                                    if error.api_code == 187:
                                        # Do something special
                                        print('duplicate message')
                            else:
                                reply = "🤖Hola @" + tweet.user.screen_name + ". En " + word + " los casos activos de Covid19 son " + str(
                                    casos_ultimo_informe) + " con base en el último informe epidemiológico del @ministeriosalud (" + fecha + "), " + str(
                                    (-1) * variacion) + " menos que en el informe anterior."
                                reply2 = "🤖Además, acorde a la información de la campaña #YoMeVacuno ✌️, un " + porcentaje1 + "% de la población objetivo tiene su primera dosis, un " + porcentaje2 + "% tiene pauta completa, y un " + porcentaje3 + "% tiene dosis de refuerzo 🦾."
                                reply3 = "🤖Por último, un promedio diario de " + promedio + " personas han recibido su vacuna en " + word + " esta semana 🦾."
                                try:
                                    update = api.update_status(status=reply, in_reply_to_status_id=tweet.id)
                                    update2 = api.update_status(status=reply2, in_reply_to_status_id=update.id)
                                    api.update_status(status=reply3, in_reply_to_status_id=update2.id)
                                except tweepy.TweepError as error:
                                    if error.api_code == 187:
                                        # Do something special
                                        print('duplicate message')
    return new_since_id

def main(a,b,c,d):
    api = create_api(a,b,c,d)
    since_id = 1462506562694443012
    my_files = {
        'activos':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto19/CasosActivosPorComuna.csv',
        'activos_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto19/CasosActivosPorComuna_T.csv',
        'vacunacion':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_1eraDosis.csv',
        'vacunacion1_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_1eraDosis_T.csv',
        'vacunacion2_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_2daDosis_T.csv',
        'vacunacion3_comuna_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/vacunacion_comuna_edad_UnicaDosis_T.csv',
        'poblacion_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto81/poblacion_comuna_edad_T.csv',
        'vacunacion1_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_1eraDosis_T.csv',
        'vacunacion2_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_2daDosis_T.csv',
        'vacunacion3_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_UnicaDosis_T.csv',
        'vacunacionR_dosis_t':
            'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto80/vacunacion_comuna_Refuerzo_T.csv'
    }
    content = requests.get(my_files['activos']).content
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    df.dropna(subset=['Codigo comuna'], inplace=True)
    keywords = df.Comuna.unique()
    a = np.array([x.lower() if isinstance(x, str) else x for x in keywords])
    keywords = a.tolist()
    while True:
        since_id = check_mentions(api, keywords, since_id)
        logger.info("Waiting...")
        time.sleep(60)

def normalizaNombreComuna(comuna):
    # standards:
    keys = pd.read_csv('../input/Otros/comunas_name_char.csv',header=None)
    values = pd.read_csv('../input/Otros/comunas_name.csv',header=None)
    keys = keys[0].to_list()
    values = values[0].to_list()
    comunas = dict(zip(keys, values))
    for comuna_simple, comuna_especial in comunas.items():
        comuna = comuna.replace(comuna_simple, comuna_especial)
    return comuna

if __name__ == "__main__":
    consumer_key = sys.argv[1]
    consumer_secret = sys.argv[2]
    access_token = sys.argv[3]
    access_token_secret = sys.argv[4]
    main(consumer_key,consumer_secret,access_token,access_token_secret)
