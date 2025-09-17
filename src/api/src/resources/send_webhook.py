import dynamically.dynamically as main_dynamically
import json

def webhook(tag_monitoria, erro):
    path="C:\\Users\\abarbosa\\Documents\\projetos\\dados-pipeline-api-thunders\\src\\resources"
    path_local= "/home/administrador/projetos/dados-pipeline-api-thunders/src/resources"
    with open(f"{path_local}/webhook.json", "r", encoding="utf-8") as config_file:
        config = json.load(config_file)    
        
    config["webhook"]["parametros_webhook"]["tag_monitoria"] = tag_monitoria
    config["webhook"]["parametros_webhook"]["erro"] = erro
    webhook_url = config["webhook"]["msg_webhook"]["url_webhook"]

    info = config["webhook"]["msg_webhook"]["info1"]
    dash = config["webhook"]["msg_webhook"]["dash"]
    # print(info)
    main_dynamically.send_webhook_message(webhook_url, dash, info.format(**config["webhook"]["parametros_webhook"]))
    