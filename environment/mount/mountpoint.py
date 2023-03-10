# Databricks notebook source
# MAGIC %md # Mount Point

# COMMAND ----------

# MAGIC %md ### Criando conexão com o adls

# COMMAND ----------

config =  {"fs.azure.account.key.adlsprdbootcamp.blob.core.windows.net" :dbutils.secrets.get(scope = "scopodbwbootcamp", key = "secret-key-adls-bootcamp")}

# COMMAND ----------

# DBTITLE 1,Lista de diretórios do lake 
#apenas os diretórios que vamos interagir
diretorios = ['delta','transient','landing']

# COMMAND ----------

# DBTITLE 1,Desmontando as conexões com o lake
def unmount_diretorio_lake(lst_diretorios):
    try:
        for diretorio in lst_diretorios:
            dbutils.fs.unmount( f"/mnt/{diretorio}/")
            print(f"{diretorio} = ok")
            
    except ValueError as error:
        print(error)
        
unmount_diretorio_lake(diretorios)   

# COMMAND ----------

# DBTITLE 1,Criando o mountpoint entre o lake e o databricks
def mount_diretorio_lake(lst_diretorios):
    try:        
        for diretorio in lst_diretorios:
            dbutils.fs.mount(
                source = f"wasbs://{diretorio}@adlsprdbootcamp.blob.core.windows.net"
                ,mount_point = f"/mnt/{diretorio}/"
                ,extra_configs = config
            )
            print(f"{diretorio} = ok")
            
    except ValueError as error:
        print(error)
        
mount_diretorio_lake(diretorios)        

# COMMAND ----------

# DBTITLE 1,listando diretórios no DBFS com o dbutils
dbutils.fs.ls("/mnt/delta/")
