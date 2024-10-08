# -*- coding: utf-8 -*-

## imports
import requests
import os, io, sys
import sqlite3
import time
import subprocess
import json
import pandas as pd
import dotenv
from tabulate import tabulate
from datetime import datetime

## NECESSARIO INSTALACAO DO AZ CLI
# fonte: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?pivots=apt
# UBUNTU - curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
# Instuções de Login az cli: https://www.ibm.com/docs/pt-br/guardium-insights/3.2.x?topic=azure-log-in-cli

## Local raiz da aplicacao
dirapp = os.path.dirname(os.path.realpath(__file__))


## Verifica sistema operacional
def verifyPlatform():
    return sys.platform


## funcao para ler valores do DOTENV
def obterValoresDotEnv():
    
    ## Carrega os valores do .env
    dotenv.load_dotenv()

    value_subscriptionid = os.getenv("VALUE_SUBSCRIPTION_AZURE")
    value_resourcegroup  = os.getenv("VALUE_RESOURCEGROUP_AZURE")
    value_azureserver    = os.getenv("VALUE_SERVER_AZURE")

    return value_subscriptionid, value_resourcegroup, value_azureserver


## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora


## funcao que obtem do timestamp atual
def obterTimeStampNow():
    current_timestamp = time.time()
    return int(current_timestamp)


## funcao que grava o timestamp expire e token obtidos para arquivos txt
def gravarTokenTimeStampExpire():

    valueToken, valueTimeStamp = geraTokenApi()

    ## grava timestamp expireon
    pathTimeStamp = os.path.join(dirapp, 'dirts')
    pathTimeStampFile = os.path.join(pathTimeStamp, 'ts_expireon.txt')

    if not os.path.exists(pathTimeStamp):
        os.makedirs(pathTimeStamp)
    else:
        pass

    with io.open(pathTimeStampFile, 'w', encoding='utf-8') as file:
        valueTimeStamp = int(valueTimeStamp)
        file.write(str(valueTimeStamp))

    
    ## grava token api
    pathToken = os.path.join(dirapp, 'token')
    pathTokenFile = os.path.join(pathToken, 'tokenApi.txt')

    if not os.path.exists(pathToken):
        os.makedirs(pathToken)
    else:
        pass

    with io.open(pathTokenFile, 'w', encoding='utf-8') as file:
        file.write(str(valueToken))

    return valueToken, valueTimeStamp


## funcao que le o timestamp expire do arquivo txt
def lerTimeStampExpire(pathTimeStampFile):
    """
    Le o timestamp do arquivo ts_expireon.txt
    Caso o arquivo esteja vazio (v_timestampexpire == '') gera novo token e timestamp
    """
        
    with io.open(pathTimeStampFile, 'r', encoding='utf-8') as f:
        v_timestampexpire = f.read()

    if v_timestampexpire == '':
        v_timestampexpire = gravarTokenTimeStampExpire()[1]

    return v_timestampexpire


## funcao que le o token do arquivo txt
def lerTokenApi(pathTokenFile):
    """
    Le o token api do arquivo tokenApi.txt
    Caso o arquivo esteja vazio (v_token == '') gera novo token e timestamp
    """

    with io.open(pathTokenFile, 'r', encoding='utf-8') as f:
        v_token = f.read()

    if v_token == '':
        v_token = gravarTokenTimeStampExpire()[0]

    return v_token


## funcao que obtem do azure cli o token api
def geraTokenApi():

    value_subscriptionid, value_resourcegroup, value_azureserver = obterValoresDotEnv()

    value_platform = str(verifyPlatform())
    cmd = 'az account get-access-token --subscription {0}'.format(value_subscriptionid)
    
    if value_platform == 'win32':
        msg = 'Sistema Operacional: {0}'.format(value_platform)
        GravaLog(msg, 'a')
        out = subprocess.run(["cmd", "/c", cmd], capture_output=True, text=True)
        valuesJson = json.loads(out.stdout)
        valueToken = valuesJson['accessToken']
        #valueExpireTokenTimeStamp = valuesJson['expires_on']
        valueExpireOn = valuesJson['expiresOn']
        valueExpireOn = datetime.strptime(valueExpireOn, "%Y-%m-%d %H:%M:%S.%f")
        valueExpireTokenTimeStamp = valueExpireOn.timestamp()
    
    elif value_platform == 'linux':
        msg = 'Sistema Operacional: {0}'.format(value_platform)
        GravaLog(msg, 'a')
        out = subprocess.run(['az', 'account', 'get-access-token', '--subscription', value_subscriptionid], capture_output=True, text=True)
        valuesJson = json.loads(out.stdout)
        valueToken = valuesJson['accessToken']
        #valueExpireTokenTimeStamp = valuesJson['expires_on']
        valueExpireOn = valuesJson['expiresOn']
        valueExpireOn = datetime.strptime(valueExpireOn, "%Y-%m-%d %H:%M:%S.%f")
        valueExpireTokenTimeStamp = valueExpireOn.timestamp()

    elif value_platform == 'darwin':
        """
        Necessita testes em ambiente macOS
        """
        msg = 'Sistema Operacional: {0}'.format(value_platform)
        print(GravaLog(msg, 'a'))
        out = subprocess.run(['az', 'account', 'get-access-token', '--subscription', value_subscriptionid], capture_output=True, text=True)
        valuesJson = json.loads(out.stdout)
        valueToken = valuesJson['accessToken']
        #valueExpireTokenTimeStamp = valuesJson['expires_on']
        valueExpireOn = valuesJson['expiresOn']
        valueExpireOn = datetime.strptime(valueExpireOn, "%Y-%m-%d %H:%M:%S.%f")
        valueExpireTokenTimeStamp = valueExpireOn.timestamp()
    else:
        msg = 'Sistema operacional não definido. Saindo da aplicação.'
        GravaLog(msg, 'a')
        exit()
    
    return valueToken, valueExpireTokenTimeStamp


def obterTokenAzure():

    """
    funcao que verifica a necessidade de gerar novo token
    1 - caso nao existe o arquivo txt do token ou timestamp expire ele cria o token inicial
    2 - caso timestamp atual do server seja maior que o timestamp expire do token faz a renovacao do token
    3 - caso o token ainda exteja com timestamp expire valido usa o meso token do arquivo txt
    """

    ## diretorio do arquivo ts_expireon.txt
    pathTimeStamp = os.path.join(dirapp, 'dirts')
    pathTimeStampFile = os.path.join(pathTimeStamp, 'ts_expireon.txt')

    ## diretorio do arquivo tokenApi.txt
    pathToken = os.path.join(dirapp, 'token')
    pathTokenFile = os.path.join(pathToken, 'tokenApi.txt')

    if not os.path.exists(pathTimeStampFile) or not os.path.exists(pathTokenFile): 
        msgTokenAviso = 'Token inicial criado.'
        valueToken, valueTimeStamp = gravarTokenTimeStampExpire()

    else:
        v_timestampnow = int(obterTimeStampNow())
        v_timestampexpire = int(lerTimeStampExpire(pathTimeStampFile))
        
        if v_timestampnow > v_timestampexpire:
            msgTokenAviso = 'Renovado token devido ao timestamp expirado.'
            valueToken, valueTimeStamp = gravarTokenTimeStampExpire()
        
        else:
            msgTokenAviso = 'TimeStamp ainda valido.'
            valueToken = lerTokenApi(pathTokenFile)

    GravaLog(msgTokenAviso, 'a')
    return valueToken


## funcao de gravacao de log
def GravaLog(strValue, strAcao):

    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'loginfoDatabaseAzureSqlApi_{0}.txt'.format(datahoraLog))

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## obter dados da Api Azure SQL
def obterDadosAzureSqlApi():

    value_subscriptionid, value_resourcegroup, value_azureserver = obterValoresDotEnv()

    v_token = str(obterTokenAzure())

    url = "https://management.azure.com/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.Sql/servers/{2}/databases?api-version=2021-11-01".\
        format(value_subscriptionid, value_resourcegroup, value_azureserver)

    headers = {
        "Authorization": "Bearer {0}".format(v_token),
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    #print(data)
    return data


## obter dados do json e transforma em list python
def jsonToListAzureSQLData(p_data):
    #print(data)
    
    #i = 0
    listDbs = []
    listDbsAux = []

    for functions in p_data["value"]:
        v_namedb = functions['name']
        v_tiername = functions['sku']['name']
        v_tier = functions['sku']['tier']
        v_capacity = functions['sku']['capacity']
        v_currentServiceObjectiveName = functions['properties']['currentServiceObjectiveName']
        v_status = functions['properties']['status']
        v_location = functions['location']
        v_collation = functions['properties']['collation']
     
        strListValues = '{0},{1},{2},{3},{4},{5},{6},{7}'\
            .format(v_namedb, v_tiername, v_tier, v_capacity, v_currentServiceObjectiveName, v_status, v_location, v_collation)

        listDbsAux = strListValues.split(',')
        listDbs.append(listDbsAux)

        #str(data["value"][i]["name"])
        #i = i + 1

    return listDbs


## Funcao de criacao do database e tabela caso nao exista
def create_tables(dbname_sqlite3):
    """
    script sql de criacao da tabela
    pode ser adicionado a criacao de mais de uma tabela
    separando os scripts por virgulas
    """
    sql_statements = [
        """
        CREATE TABLE "infoDatabaseAzureSqlApi" (
            "infoDatabaseAzureSqlApiId"	INTEGER NOT NULL UNIQUE,
            "Database"	TEXT NOT NULL,
            "TierName"	TEXT NOT NULL,
            "Tier"	TEXT NOT NULL,
            "Capacity"	NUMERIC,
            "CurrentServiceObjectiveName"	TEXT NOT NULL,
            "Status"	TEXT NOT NULL,
            "Location"  TEXT NOT NULL,
            "Collation"	TEXT NOT NULL,
            "UltimaVerificacao" REAL NOT NULL,
            PRIMARY KEY("infoDatabaseAzureSqlApiId" AUTOINCREMENT)
        )        
        """
    ]

    # variaveis da conexão ao database
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    
    # cria o diretorio caso nao exista
    if not os.path.exists(path_dir_db):
        os.makedirs(path_dir_db)
    else:
        pass
    

    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
            
            conn.commit()
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Criar tabela SQlite3 [infoDatabaseAzureSqlApi] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))
    finally:
        msgLog = 'Criado tabela [infoDatabaseAzureSqlApi] no database [{0}]'.format(dbname_sqlite3)
        print(GravaLog(msgLog, 'a'))


## gera comandos de inserts conforme valores da lista passada
def gravaDadosSqlite(v_ListValuesMongoDB):
    dbname_sqlite3 = "database_bi.db"
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    RowCount = 0

    ## verifica se banco de dados existe 
    # caso não exista realizada a chamada da funcao de criacao
    if not os.path.exists(path_dir_db):
        create_tables(dbname_sqlite3)
    else:
        pass

    
    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            cur = conn.cursor()

            ## sql statement DELETE
            sqlcmdDELETE = 'DELETE FROM infoDatabaseAzureSqlApi;'
            cur.execute(sqlcmdDELETE)
            RowCountDelete = conn.total_changes
            conn.commit()
            
            ## sql statement INSERT
            sqlcmdINSERT = '''
            INSERT INTO infoDatabaseAzureSqlApi
                (Database, TierName, Tier, Capacity, CurrentServiceObjectiveName, Status, Location, Collation, UltimaVerificacao) 
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'));
            '''
            cur.executemany(sqlcmdINSERT, v_ListValuesMongoDB)
            RowCountInsert = conn.total_changes
            conn.commit()
    
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela SQlite3 [infoDatabaseAzureSqlApi] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        RowCount = RowCountInsert - RowCountDelete
        msgLog = 'Quantidade de Registros Inseridos na tabela [infoDatabaseAzureSqlApi]: {0} registro(s)'.format(RowCount)
        print(GravaLog(msgLog, 'a'))


def exibeDadosSqlite():
    dbname_sqlite3 = "database_bi.db"
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    
    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            sqlcmd = """
            SELECT 
                Database,
                TierName,
                --Tier,
                Capacity,
                CurrentServiceObjectiveName AS "CurrentService",
                Status,
                Location, 
                Collation,
                UltimaVerificacao
            FROM infoDatabaseAzureSqlApi 
            --WHERE Database != 'master';
            """

            df = pd.read_sql(sqlcmd, conn)
            v_out_table = tabulate(df, headers='keys', tablefmt='psql', showindex=False)
            print(GravaLog(v_out_table, 'a'))

    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Select tabela SQlite3 [infoDatabaseAzureSqlApi] [Erro]: {0}\n{1}'.format(datahora, msgException)
        print(GravaLog(msgLog, 'a'))

    finally:
        msgLog = 'Fim Select tabela SQlite3 [infoDatabaseAzureSqlApi]'
        print(GravaLog(msgLog, 'a'))


## FUNCAO INICIAL
def main():
    ## log do inicio da aplicacao
    datahora = obterDataHora()
    msgLog = '\n***** Inicio da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))
    
    ## obter dados da api azuresql
    v_data = obterDadosAzureSqlApi()
    if "The access token is invalid." in str(v_data):
        msgLog = 'Error de validação do token: {0}'.format(v_data)
        print(GravaLog(msgLog, 'a'))
    else:
        ## receber dados tratados
        v_listDbs = jsonToListAzureSQLData(v_data)

        if v_listDbs:

            ## grava dados no database de destino
            gravaDadosSqlite(v_listDbs)

            ## exibe os dados em formato tabela
            exibeDadosSqlite()
        else:
            pass

    ## log do final da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Final da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da funcao inicial
    main()
    